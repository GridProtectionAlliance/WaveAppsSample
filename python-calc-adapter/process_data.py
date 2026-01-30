# ******************************************************************************************************
#  process-data.py - Gbtc
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
#  01/09/2024 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

import numpy as np
import uuid

from data_proxy import DataProxy
from sttp.ticks import Ticks
from sttp.transport.measurement import Measurement
from typing import Dict, Final
from uuid import UUID

# Alarm event value constants
RAISED: Final = 1.0
""" Indicates alarm for event is raised. """

CLEARED: Final = 0.0
""" Indicates alarm for event is cleared. """

# Other constants used in example
POWER_ESTIMATE_RATIO: Final = 19530.0  # MW per Hz deviation
FREQ_MIN: Final = 59.95
FREQ_MAX: Final = 60.05
NOMINAL_FREQ: Final = 60.0

def process_data(data_proxy: DataProxy, timestamp: np.uint64, databuffer: Dict[np.uint64, Dict[UUID, Measurement]]):
    """
    User function that processes time-aligned grouped data that has been received.

    Note: This function is called when grouped data is available for processing. The function will only
    be called once per second with a buffer of grouped data for the second. If the function processing
    time exceeds the one second window, a warning message will be displayed and new data will be skipped.
    The number of skipped data sets is tracked and reported through the `processmissedcount` property.
    
    Parameters:
        timestamp:   The timestamp, at top of second, for the grouped data
        data_buffer: The grouped one second data buffer:
                     np.uint64: sub-second timestamps of aligned measurement groups
                     Dict[UUID, Measurement]: aligned measurements for the sub-second timestamp
    """

    # In this example, we calculate average frequency for all frequencies in the one second buffer
    frequency_sum = 0.0
    frequency_count = 0

    # Loop through each set of measurement groups in the one second buffer
    for measurements in databuffer.values():
        # To use subsecond timestamp values, you can use the following loop instead:
        #     for subsecond_timestamp, measurements in data_buffer.items():

        # subsecond_timestamp is the timestamp rounded to the nearest subsecond distribution.
        # Milliseconds of the timestamp at 30 samples per second are 0, 33, 66, or 100 ms, etc.
        # For example:
        #    2024-07-30 17:55:29.233
        #    2024-07-30 17:55:29.266
        #    2024-07-30 17:55:29.333
        #    2024-07-30 17:55:29.366

        # At this point, all measurements are aligned to the same subsecond timestamp

        # If you know which measurement you are looking for, you can use the following lookup:
        #     measurement = measurements.get(my_signalid)

        # Loop through each measurement in the subsecond time-aligned group
        for measurement in measurements.values():
            # To use UUID values, you can use the following loop instead:
            #     for signalid, measurement in measurements.items():

            # Note:
            #   measurement.value is a numpy float64
            #   measurement.timestamp is a numpy uint64 (in ticks, i.e., 100-nanosecond intervals)
            #    - use Ticks.to_string to convert to a human readable string
            #    - use Ticks.to_datetime to convert to a Python datetime
            #   measurement.signalid is a UUID
            #    - use str(measurement.signalid) to convert to a human readable string
            #    - use self.measurement_metadata to get associated MeasurementRecord
            #
            # See measurement.py for more details

            # Ensure frequency is in reasonable range (59.95 to 60.05 Hz) and not NaN
            if not np.isnan(measurement.value) and measurement.value >= 59.95 and measurement.value <= 60.05:
                # The following line demonstrates how to use the value of a measurement based on its
                # linear adjustment factor metadata , i.e., the configured adder and multiplier:
                #frequency_sum += data_proxy.adjustedvalue(measurement)                
                frequency_sum += measurement.value # raw, unadjusted value
                frequency_count += 1

    if frequency_count == 0:
        data_proxy.statusmessage(f"\nNo valid frequency measurements received in second {Ticks.to_datetime(timestamp).second}.")
        return
    
    avg_frequency = frequency_sum / frequency_count

    data_proxy.statusmessage(f"\nAverage frequency for {frequency_count:,} values in second {Ticks.to_datetime(timestamp).second}: {avg_frequency:.6f} Hz")

    if data_proxy.downsampledcount > 0:
        data_proxy.statusmessage(f"   WARNING: {data_proxy.downsampledcount:,} measurements downsampled in last measurement set...")
        data_proxy.downsampledcount = 0

    # Publish calculated average frequency to WaveApps host
    data_proxy.publisher.publish_measurements([Measurement(
        signalid=data_proxy.avg_frequency_signalid,
        timestamp=timestamp,
        value=np.float64(avg_frequency)
    )])

    # Handle frequency excursion event detection operations
    if avg_frequency < FREQ_MIN or avg_frequency > FREQ_MAX:
        # Frequency excursion detected - publish event if one is not already active
        if data_proxy.freq_excursion_eventid is not None:
            return
        
        # Create new event ID
        data_proxy.freq_excursion_eventid = uuid.uuid4()

        # Calculate estimated MW impact based on frequency excursion
        frequency_delta = avg_frequency - NOMINAL_FREQ  # Delta would be better from previous second
        estimated_mw_impact = frequency_delta * POWER_ESTIMATE_RATIO  # Rough estimate only just for example
        
        # Update event details JSON with calculated MW impact
        event_details = f'''{{
            "description": "Frequency excursion detected with MW of estimated impact of {estimated_mw_impact:.2f} MW",
            "AverageFrequency": {avg_frequency:.6f},
            "EstimatedMW": {estimated_mw_impact:.2f}
        }}'''

        data_proxy.publish_event(
            data_proxy.freq_excursion_signalid, 
            data_proxy.freq_excursion_eventid, 
            Ticks.utcnow(), 
            timestamp, 
            RAISED, 
            event_details)
    else:
        if data_proxy.freq_excursion_eventid is None:
            return

        data_proxy.publish_event(
            data_proxy.freq_excursion_signalid, 
            data_proxy.freq_excursion_eventid,
            Ticks.utcnow(), 
            timestamp, 
            CLEARED)

        # Clear active event ID when frequency has returned to normal range
        data_proxy.freq_excursion_eventid = None