# ******************************************************************************************************
#  data_proxy.py - Gbtc
#
#  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
#  the NOTICE file distributed with this work for additional information regarding copyright ownership.
#  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
#  file except in compliance with the License. You may obtain a copy of the License at:
#
#      http://opensource.org/licenses/MIT
#
#  Unless agreed to in writing, the subject software distributed under the License is distributed on an
#  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
#  License for the specific language governing permissions and limitations.
#
#  Code Modification History:
#  ----------------------------------------------------------------------------------------------------
#  07/30/2024 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

from __future__ import annotations
import threading
import numpy as np

from sttp.publisher import Publisher
from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings
from sttp.ticks import Ticks
from sttp.transport.constants import ServerResponse, ServerCommand
from sttp.transport.measurement import Measurement
from sttp.transport.subscriberconnection import SubscriberConnection
from sttp.transport.signalindexcache import SignalIndexCache
from typing import Callable, List, Dict
from uuid import UUID
from time import time

class DataProxy(Subscriber):
    """
    Data proxy groups data by timestamp to the nearest second and publishes grouped data after a specified measurement
    window size has elapsed. Data is grouped by timestamp to the nearest second and then further grouped by subsecond
    distribution based on the number of samples per second. The grouped data is then published to a user defined callback
    function that handles the grouped data.

    If incoming frame rate is higher than the samples per second, or timestamp alignment does not accurately coinside with
    the subsecond distribution, some data will be downsampled. Downsampled data count is tracked and reported to through the
    `downsampledcount` property.

    Only a single one-second data buffer will be published at a time. If data cannot be processed within the one-second
    window, a warning message will be displayed and any new data will be skipped. The number of skipped data sets is tracked
    and reported through the `processmissedcount` property.

    This example depends on a semi-accurate system clock to group data by timestamp. If the system clock is not accurate,
    data may not be grouped as expected.

    Additionally, this implementation handles data publication back to WaveApps host application.

    NOTE: This is internal functionality used to send and receive data from the WaveApps host application. It is not expected
    that user will need to modify this code.
    """

    def __init__(self):
        super().__init__()

        # Configure STTP subscriber settings
        self.config = Config()
        self.settings = Settings()

        self.measurement_windowsize = 5
        """
        Defines measurement window size, in whole seconds, for data grouping.
        """

        self.lagtime = 2.0
        """
        Defines the lag time, in seconds, for data grouping. Data received outside
        of this past time limit, relative to local clock, will be discarded.
        """

        self.leadtime = 2.0
        """
        Defines the lead time, in seconds, for data grouping. Data received outside
        this future time limit, relative to local clock,  will be discarded.
        """

        self.samplespersecond = 30
        """
        Defines the number of samples per second for the data in the stream.
        """

        self.display_measurementsummary = False
        """
        Defines flag that determines if the data proxy should display a summary of
        received measurements every few seconds.
        """
        
        self._groupeddata: Dict[np.uint64, Dict[np.uint64, Dict[UUID, Measurement]]] = {} 
        self._groupeddata_receiver: Callable[[DataProxy, np.uint64, Dict[np.uint64, Dict[UUID, Measurement]]]] | None = None

        self._lastmessage = 0.0

        self._downsampledcount_lock = threading.Lock()
        self._downsampledcount = 0

        self._process_lock = threading.Lock()
        
        self._processmissedcount_lock = threading.Lock()
        self._processmissedcount = 0

        # Set up event handlers for STTP API
        self.subscriptionupdated_receiver = self._subscription_updated
        self.newmeasurements_receiver = self._new_measurements
        self.connectionterminated_receiver = self._connection_terminated

        self.publisher = Publisher()

        self.publisher.statusmessage_logger = self._publisher_status
        self.publisher.errormessage_logger = self._publisher_error
        self.publisher.clientconnected_receiver = self._publisher_client_connected
        self.publisher.clientdisconnected_receiver = self._publisher_client_disconnected

    @property
    def downsampledcount(self) -> int:
        """
        Gets the count of downsampled measurements.
        """

        with self._downsampledcount_lock:
            return self._downsampledcount

    @downsampledcount.setter
    def downsampledcount(self, value: int):
        """
        Sets the count of downsampled measurements.
        """

        with self._downsampledcount_lock:
            self._downsampledcount = value

    @property
    def processmissedcount(self) -> int:
        """
        Gets the count of missed data processing.
        """

        with self._processmissedcount_lock:
            return self._processmissedcount
        
    @processmissedcount.setter
    def processmissedcount(self, value: int):
        """
        Sets the count of missed data processing.
        """

        with self._processmissedcount_lock:
            self._processmissedcount = value

    def start(self, port: int, ipv6: bool = False):
        """
        Starts the publisher proxy listening on the specified port.
        
        Parameters
        ----------
        port : int
            TCP port number to listen on
        ipv6 : bool, optional
            Set to True to use IPv6, False for IPv4 (default)
        """
        self.publisher.start(port, ipv6)

    def dispose(self):
        """
        Cleanly shuts down a `DataProxy` that is no longer being used, e.g., during a normal application exit.
        """

        if self.publisher is not None:
            self.publisher.dispose()

        return super().dispose()

    def set_groupeddata_receiver(self, callback: Callable[[DataProxy, np.uint64, Dict[np.uint64, Dict[UUID, Measurement]]], None] | None):
        """
        Defines the callback function that processes grouped data that has been received.

        Function signature:
            def process_data(DataProxy subscriber, timestamp: np.uint64, databuffer: Dict[np.uint64, Dict[UUID, Measurement]]):
                pass
        """

        self._groupeddata_receiver = callback

    def publish_event(self, timestamp: np.uint64, alarm_timestamp: np.uint64, value: float | np.float64, event_details_json: str):
        """
        Publishes an event to connected WaveApps host application clients.

        Parameters:
            timestamp:          The event timestamp in ticks (100-nanosecond intervals since 1/1/0001)
            alarm_timestamp:    The alarm timestamp in ticks (100-nanosecond intervals since 1/1/0001)
            value:              The event value
            event_details_json: The event details in JSON format
        """

        # TODO: Implement event publishing back to WaveApps host application
        # EventDataType = ...
        #self.publisher.broadcast_response(ServerResponse.USERRESPONSE01, ServerCommand.USERCOMMAND01, binary_image)
        pass

    def _timeisvalid(self, timestamp: np.uint64) -> bool:
        """
        Determines if the given timestamp is within the valid time range for data grouping.
        """
        distance = Ticks.utcnow() - timestamp
        leadtime = self.leadtime * Ticks.PERSECOND
        lagtime = self.lagtime * Ticks.PERSECOND

        return bool(distance >= -leadtime and distance <= lagtime)

    def _round_to_nearestsecond(self, timestamp: np.uint64) -> np.uint64:
        """
        Rounds the timestamp rounded to the nearest second.
        """
        
        return timestamp - timestamp % Ticks.PERSECOND

    def _round_to_subseconddistribution(self, timestamp: np.uint64) -> np.uint64:
        """
        Rounds the timestamp to the nearest subsecond distribution based on the number of samples per second.
        """
       
        # Baseline timestamp to the top of the second
        base_ticks = self._round_to_nearestsecond(timestamp)

        # Remove the seconds from ticks
        ticks_beyond_second = timestamp - base_ticks

        # Calculate a frame index between 0 and m_framesPerSecond - 1,
        # corresponding to ticks rounded to the nearest frame
        frame_index = np.round(ticks_beyond_second / (Ticks.PERSECOND / self.samplespersecond))

        # Calculate the timestamp of the nearest frame
        destination_ticks = np.uint64(frame_index * Ticks.PERSECOND / self.samplespersecond)

        # Recover the seconds that were removed
        destination_ticks += base_ticks

        return destination_ticks

    def _subscription_updated(self, signalindexcache: SignalIndexCache):
        self.statusmessage(f"Received signal index cache with {signalindexcache.count:,} mappings")

    def _new_measurements(self, measurements: List[Measurement]):
        # Collect data into a map grouped by timestamps to the nearest second
        for measurement in measurements:
            # Get timestamp rounded to the nearest second
            timestamp_second = self._round_to_nearestsecond(measurement.timestamp)

            if self._timeisvalid(timestamp_second):
                # Create a new one-second timestamp map if it doesn't exist
                if timestamp_second not in self._groupeddata:
                    self._groupeddata[timestamp_second] = {}

                # Get timestamp rounded to the nearest subsecond distribution, e.g., 000, 033, 066, 100 ms
                timestamp_subsecond = self._round_to_subseconddistribution(measurement.timestamp)

                # Create a new subsecond timestamp map if it doesn't exist                
                if timestamp_subsecond not in self._groupeddata[timestamp_second]:
                    self._groupeddata[timestamp_second][timestamp_subsecond] = {}

                # Append measurement to subsecond timestamp list, tracking downsampled measurements
                if measurement.signalid in self._groupeddata[timestamp_second][timestamp_subsecond]:
                    with self._downsampledcount_lock:
                        self._downsampledcount += 1

                self._groupeddata[timestamp_second][timestamp_subsecond][measurement.signalid] = measurement

        # Check if it's time to publish grouped data, waiting for measurement_windowsize to elapse. Note
        # that this implementation depends on continuous data reception to trigger data publication.
        # A more robust implementation would use a precision timer to trigger data publication.
        currenttime = Ticks.utcnow()
        windowsize = np.uint64(self.measurement_windowsize * Ticks.PERSECOND)

        for timestamp in list(self._groupeddata.keys()):
            if currenttime - timestamp >= windowsize:
                groupeddata = self._groupeddata.pop(timestamp)

                # Call user defined data function handler with one-second grouped data buffer on a separate thread
                threading.Thread(target=self._publish_data, args=(timestamp, groupeddata), name="PublishDataThread").start()
 
        # Provide user feedback on data reception
        if not self.display_measurementsummary or time() - self._lastmessage < 5.0:
            return

        try:
            if self._lastmessage == 0.0:
                self.statusmessage("Receiving measurements...")
                return

            message = [
                f"{self.total_measurementsreceived:,} measurements received so far...\n",
                f"Timestamp: {Ticks.to_string(measurements[0].timestamp)}\n",
                "\tID\tSignal ID\t\t\t\tValue\n"
            ]

            for measurement in measurements:
                metadata = self.measurement_metadata(measurement)
                
                if metadata is not None:
                    message.append(f"\t{metadata.id}\t{measurement.signalid}\t{measurement.value:.6}\n")

            self.statusmessage("".join(message))
        finally:
            self._lastmessage = time()

    def _publish_data(self, timestamp: np.uint64, databuffer: Dict[np.uint64, Dict[UUID, Measurement]]):
        databuffer_timestr = Ticks.to_shortstring(timestamp).split(".")[0]

        if self._process_lock.acquire(False):
            try:
                processstarted = time()

                if self._groupeddata_receiver is not None:
                    self._groupeddata_receiver(self, timestamp, databuffer)

                self.statusmessage(f"Data publication for buffer at {databuffer_timestr} processed in {self._elapsed_timestr(time() - processstarted)}.")
            finally:
                self._process_lock.release()
        else:
            with self._processmissedcount_lock:
                self._processmissedcount += 1
                self.errormessage(f"WARNING: Data publication missed for buffer at {databuffer_timestr}, a previous data buffer is still processing. {self._processmissedcount:,} data sets missed so far...")

    def _elapsed_timestr(self, elapsed: float) -> str:
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        milliseconds = (elapsed - int(elapsed)) * 1000

        if hours < 1.0:
            if minutes < 1.0:
                if seconds < 1.0:
                    return f"{int(milliseconds):03} ms"

                return f"{int(seconds):02}.{int(milliseconds):03} sec"
                        
            return f"{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"
                        
        return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"

    def _connection_terminated(self):
        # Call default implementation which will display a connection terminated message to stderr
        self.default_connectionterminated_receiver()

        # Reset last message display time on disconnect
        self._lastmessage = 0.0

        # Reset grouped data on disconnect
        self.downsampledcount = 0

        # Reset process missed count on disconnect
        self.processmissedcount  = 0

    def _publisher_status(self, message):
        self.statusmessage(f"[PUB] {message}")
    
    def _publisher_error(self, message):
        self.errormessage(f"[PUB] ERROR: {message}")
    
    def _publisher_client_connected(self, connection: SubscriberConnection):
        self.statusmessage(f"[PUB] Client connected: {connection.connection_id}")
    
    def _publisher_client_disconnected(self, connection: SubscriberConnection):
        self.statusmessage(f"[PUB] Client disconnected: {connection.connection_id}")