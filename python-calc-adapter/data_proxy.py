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
# pyright: reportAttributeAccessIssue=false

from __future__ import annotations
import json
import math
import os
import threading
import uuid
import numpy as np

from gsf import static_init
from sttp.publisher import Publisher
from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings
from sttp.ticks import Ticks
from sttp.metadata.signaltype import SignalType
from sttp.transport.constants import ServerResponse, ServerCommand
from sttp.transport.measurement import Measurement
from sttp.transport.subscriberconnection import SubscriberConnection
from sttp.transport.signalindexcache import SignalIndexCache
from typing import Any, Callable, List, Dict
from uuid import UUID
from time import time
from data_proxy_publisher import DataProxy_Publisher, DeviceDef, MeasurementDef

@static_init
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

    NOTE: This class defines internal functionality used to send and receive data from the WaveApps host application. It is
    not expected that user will need to modify this code save for adding custom calculation output details and configuration
    parameters below (see TODO: items).
    """

    _connection_string_params = {}

    def __init__(self, config: Callable[['DataProxy'], None] = None):
        super().__init__()

        # Define publisher for sending data back to WaveApps host application
        publisher = DataProxy_Publisher()

        # # Ensure calculation output source is defined
        # self.device_acronym = publisher.define_output_source("AvgFreqCalc")
        # """
        # The device acronym for the calculation output measurements.
        # """

        # # Ensure calculation output measurements are defined
        #self.avg_frequency_signalid = publisher.define_output_measurement(self.device_acronym, "AVG_FREQ", "Calculated Average Frequency")
        # """
        # The signal identifier for the average frequency measurement.
        # """

        # self.freq_excursion_signalid = publisher.define_output_measurement(self.device_acronym, "FREQ_EXCURSION", "Frequency Excursion Event", SignalType.ALRM)
        # """
        # The signal identifier for the frequency excursion event measurement.
        # """
 
        # -------------------------------------------------------------
        # Define calculation configuration parameters:

        self.measurement_windowsize: float = self._register_param('measurement_windowsize', 5.0)
        """
        Defines measurement window size, in whole seconds, for data grouping.
        """

        self.lagtime: float = self._register_param('lagtime', 2.0)
        """
        Defines the lag time, in seconds, for data grouping. Data received outside
        of this past time limit, relative to local clock, will be discarded.
        """

        self.leadtime: float = self._register_param('leadtime', 2.0)
        """
        Defines the lead time, in seconds, for data grouping. Data received outside
        this future time limit, relative to local clock, will be discarded.
        """

        self.display_measurementsummary: bool = self._register_param('display_measurementsummary', False)
        """
        Defines flag that determines if the data proxy should display a summary
        of received measurements every few seconds.
        """
        
        self.samplespersecond: float = self._register_param('samplespersecond', 3000)
        """
        Defines the number of samples per second for the data in the stream.
        """

        if config is not None:
            config(self) 

        # -------------------------------------------------------------

        # Configure STTP subscriber settings
        self.config = Config()
        """
        Configuration settings for the subscriber connection to WaveApps host application.
        """

        # Explicitly request TSSC payload compression. The C# WaveApps host publisher
        # only encodes with TSSC when BOTH its `AllowPayloadCompression` is true AND
        # this subscriber's request includes `CompressPayloadData` in OperationalModes
        # (i.e., compress_payloaddata=True here). Without both, the wire format falls
        # back to the uncompressed CompactMeasurement encoding. TSSC is preferred for
        # streaming measurements, so keep this true unless intentionally disabling.
        # Default values from `Config()` already set these to True; we set them again
        # explicitly so future readers see the requirement spelled out.
        self.config.compress_payloaddata = True
        self.config.compress_metadata = True
        self.config.compress_signalindexcache = True

        self.settings = Settings()
        """
        Subscription settings for the subscriber connection to WaveApps host application.
        """

        # Internal variables
        self._groupeddata: Dict[np.uint64, Dict[np.uint64, Dict[UUID, Measurement]]] = {} 
        self._groupeddata_receiver: Callable[[DataProxy, np.uint64, Dict[np.uint64, Dict[UUID, Measurement]]], None] | None = None

        self._lastmessage = 0.0

        self._downsampledcount_lock = threading.Lock()
        self._downsampledcount = 0

        self._process_lock = threading.Lock()
        
        self._processmissedcount_lock = threading.Lock()
        self._processmissedcount = 0

        self._first_window_received = 0
        self._lastWindowProcessed = 0

        """
        Counter to track the number of data batches processed.
        """
        self._batch_counter = 0

        # Set up event handlers for STTP subscriber API
        self.subscriptionupdated_receiver = self._subscription_updated
        self.newmeasurements_receiver = self._new_measurements
        self.connectionterminated_receiver = self._connection_terminated
        self.userresponse_receiver = self._user_response

        # Set up event handlers for STTP publisher API
        publisher.statusmessage_logger = self._publisher_status
        publisher.errormessage_logger = self._publisher_error
        publisher.clientconnected_receiver = self._publisher_client_connected
        publisher.clientdisconnected_receiver = self._publisher_client_disconnected
        publisher.usercommand_receiver = self._publisher_usercommand_receiver

        # Maintain a reference to the publisher
        self.publisher = publisher


        #Update Metadata cache to includ Phasor for point on wave
        self.metadatanotification_receiver = self._update_pow_phasor

        """
        Reference to the STTP publisher API used to send data back to WaveApps host application.
        """
        
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

    def publish_event(self, signalid: UUID, eventid: UUID, event_type: str, start_time: np.uint64, end_time: np.uint64, event_details: str = ""):
        """
        Publishes an event to connected WaveApps host application clients.

        The event is transported as an STTP ``BufferBlock`` measurement addressed to
        ``signalid``. The payload is a UTF-8 JSON document carrying ``EventID``, ``Type``,
        ``StartTime``, ``EndTime``, ``Value``, and ``EventDetails``. ``SignalID`` is implicit in
        the BufferBlock's signal-index header and is not duplicated in the payload. The JSON
        schema matches the substation-to-central transport in
        ``openHistorian/waveAppsDataTransfer`` so a single subscriber implementation can decode
        events from either source.

        ``REQUIRE CONFIRMATION`` is set on each event so the gsfapi publisher caches the block
        for retransmission if the subscriber's ack is lost - events are rare and high-value, so
        the round-trip overhead is well worth the delivery guarantee.

        Parameters:
            signalid:           The signal identifier for the event measurement; the BufferBlock
                                is addressed to this signal.
            eventid:            The unique event identifier (constant across the start and end
                                publications for a given event so the receiver can correlate them).
            event_type:         The event details type, e.g., the calculation source name. This
                                value is recorded as the ``Type`` field on the host
                                ``EventDetails`` record when an event starts and is used to
                                categorize events in the host application.
            start_time:         Event start time in ticks (100-nanosecond intervals since
                                1/1/0001). Pass ``0`` if not yet known / not applicable.
            end_time:           Event end time in ticks. Pass ``0`` on an event-start publication
                                where the end is not yet known.
            event_details:      The event details in JSON format (the host stores this verbatim
                                in the ``EventDetails.Details`` column).
        """

        # Serialize the event fields as a UTF-8 JSON document and broadcast as a buffer block
        # measurement. The receiver (`WaveAppsAdapter/PythonDataProxyBase.cs`) looks for
        # `BufferBlockMeasurement` instances in its `NewMeasurements` handler, parses this same
        # schema, and builds the AlarmMeasurement + EventDetails row.
        event_payload = json.dumps({
            "EventID": str(eventid),
            "Type": event_type,
            "StartTime": int(start_time),
            "EndTime": int(end_time),
            "Value": 0,
            "EventDetails": event_details,
        }).encode("utf-8")

        self.publisher.broadcast_buffer_block(signalid, event_payload, require_confirmation=True)

    def add_ouputMeasurement(self, signalid: UUID, name: str, description: str, signaltype: SignalType):
        """
        Adds an output measurement to the publisher metadata.

        Parameters:
            signalid:           The signal identifier for the output measurement.
            name:               The name of the output measurement.
            description:        The description of the output measurement.
            signaltype:         The signal type of the output measurement.
        """
        self.publisher.measurements.append(MeasurementDef(name, description, signaltype, None, signalid))
        
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

    def _round_to_windowStart(self, timestamp: np.uint64, window_size: np.uint64) -> np.uint64:
        if (self._first_window_received == 0):
            self._first_window_received = timestamp

        n = math.floor((timestamp - self._first_window_received) / window_size)

        return self._first_window_received + n * window_size

    
    def _subscription_updated(self, signalindexcache: SignalIndexCache):
        self.statusmessage(f"Received signal index cache with {signalindexcache.count:,} mappings")

    def _new_measurements(self, measurements: List[Measurement]):

        windowsize = np.uint64(self.measurement_windowsize * Ticks.PERSECOND)
        # Collect data into a map grouped by timestamps to the nearest second
        for measurement in measurements:
            # Get timestamp rounded to the nearest second
            timestamp_window = self._round_to_windowStart(measurement.timestamp, windowsize)

            if self._timeisvalid(measurement.timestamp):
                # Create a new one-second timestamp map if it doesn't exist
                if timestamp_window not in self._groupeddata:
                    self._groupeddata[timestamp_window] = {}

                # Get timestamp rounded to the nearest subsecond distribution, e.g., 000, 033, 066, 100 ms
                timestamp_subsecond = self._round_to_subseconddistribution(measurement.timestamp)

                # Create a new subsecond timestamp map if it doesn't exist                
                if timestamp_subsecond not in self._groupeddata[timestamp_window]:
                    self._groupeddata[timestamp_window][timestamp_subsecond] = {}

                # Append measurement to subsecond timestamp list, tracking downsampled measurements
                if measurement.signalid in self._groupeddata[timestamp_window][timestamp_subsecond]:
                    with self._downsampledcount_lock:
                        self._downsampledcount += 1

                self._groupeddata[timestamp_window][timestamp_subsecond][measurement.signalid] = measurement

        # Check if it's time to publish grouped data, waiting for measurement_windowsize to elapse. Note
        # that this implementation depends on continuous data reception to trigger data publication.
        # A more robust implementation would use a precision timer to trigger data publication.
        currenttime = Ticks.utcnow()
        lagtime = self.lagtime * Ticks.PERSECOND

        windows = list(self._groupeddata.keys())
        windows.sort()
        for timestamp in windows:
            if (currenttime - timestamp) >= (windowsize+ lagtime):
                groupeddata = self._groupeddata.pop(timestamp)

                if (timestamp > self._lastWindowProcessed):
                    # Call user defined data function handler with one-second grouped data buffer on a separate thread
                    threading.Thread(target=self._publish_data, args=(timestamp, groupeddata), name="PublishDataThread").start()
                    self._lastWindowProcessed = timestamp
                else:
                    self.statusmessage(f"WARNING: Data publication skipped for buffer at {Ticks.to_shortstring(timestamp).split('.')[0]} window out of order...")
 
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

                self._batch_counter += 1

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
        self.processmissedcount = 0

    def _user_response(self, response: ServerResponse, command: ServerCommand, data: bytes):
        if response == ServerResponse.USERRESPONSE02 and command == ServerCommand.USERCOMMAND02:
            connection_string = data.decode('utf-8')
            self.statusmessage(f"Received serialization of adapter properties from WaveApps host proxy publisher \"{connection_string}\"")

            # Parse connection string
            settings = dict((k.strip(), v.strip()) for item in connection_string.split(';') if '=' in item for k, v in [item.split('=', 1)])

            # Update adapter properties based on received settings
            for name, value in settings.items():
                if name in self._connection_string_params:
                    default_value = self._connection_string_params[name]

                    try:
                        parsed_value = self._parse_type(value, default_value)
                        setattr(self, name, parsed_value)
                        self.statusmessage(f"Updated adapter property \"{name}\" to value: {parsed_value}")

                    except Exception as ex:
                        self.errormessage(f"Failed to parse and set adapter property \"{name}\" with value \"{value}\": {ex}")
                else:
                    self.errormessage(f"Received unknown adapter property \"{name}\" from WaveApps host proxy publisher.")
        else:
            self.statusmessage(f"Received unhandled user response {response} for command {command} from WaveApps host proxy publisher.")

    def _parse_type(self, value: str, defaultValue: Any) -> Any:
        """
        Parses a string value into the type of the given value.
        """
        valueType = type(defaultValue)

        if valueType == bool:
            # Special handling for boolean values
            parsed_value = value.lower() in ("true", "1", "yes")
        if valueType == list:
            # Special handling for list values, assuming comma-separated values
            if (len(defaultValue) > 0):
                parsed_value = [self._parse_type(item, defaultValue[0]) for item in value.split(',')]
            else:
               parsed_value = valueType(defaultValue) 
        else:
            parsed_value = valueType(value)

        return parsed_value
    
    def _publisher_status(self, message):
        self.statusmessage(f"[PUB] {message}")
    
    def _publisher_error(self, message):
        self.errormessage(f"[PUB] ERROR: {message}")
    
    def _publisher_client_connected(self, connection: SubscriberConnection):
        self.statusmessage(f"[PUB] Client connected: {connection.connection_id}")
    
    def _publisher_client_disconnected(self, connection: SubscriberConnection):
        self.statusmessage(f"[PUB] Client disconnected: {connection.connection_id}")

    def _publisher_usercommand_receiver(self, connection: SubscriberConnection, command: ServerCommand, data: bytes):
        self.statusmessage(f"[PUB] Received user command {command} from client: {connection.connection_id}")

        if command == ServerCommand.USERCOMMAND00:
            # Client is notifying us that configuration has changed, instruct subscriber to request new metadata
            self.request_metadata()

    def _register_param(self, name: str, default_value: Any|None=None) -> Any:
        """
        Register a connection string parameter with its default value.
        If default_value is None, reads the current value from self.{name}.
        Returns the value for convenient assignment chaining.

        Parameters
        ----------
        name : str
            Name of the parameter
        default_value : any, optional
            Default value of the parameter. If None, uses getattr(self, name)

        Returns
        -------
        any
            The registered value

        Examples
        --------
        # Option 1: Assign then register
        self.lagtime: float = 2.0
        self._register_parameter('lagtime')
        
        # Option 2: Register and assign in one line (more concise)
        self.lagtime: float = self._register_parameter('lagtime', 2.0)
        """
        if default_value is None:
            # Read current value from the attribute
            value = getattr(self, name)
        else:
            # Use provided value
            value = default_value
        
        self._connection_string_params[name] = value
        return value
    
    def _update_pow_phasor(self, dataset: Dataset):
        """
        Updates the POW phasor information in the metadata cache.
        """   
        for measurement in self.metadatacache.measurement_records:
            if measurement.signalacronym == "VPOW" or measurement.signalacronym == "IPOW":
                datarow: DataRow = dataset["MeasurementDetail"].rowswhere(lambda row: row["SignalID"] == str(measurement.signalid))
                if measurement.phasor is None and len(datarow) == 1:
                    measurement.phasor = next((phasor for phasor in self.metadatacache.phasor_records if phasor.sourceindex == datarow[0]["PhasorSourceIndex"] and phasor.deviceacronym == datarow[0]["DeviceAcronym"]), None)