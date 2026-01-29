# WaveApps Python Calculation Adapter Example

This example defines a Python calculation adapter that sends data to and from the WaveApps host application.

## Key User Tasks
1. Modify `data_proxy.py`, `__init__` function, to define outputs and add any custom configuration parameters for calculation algorithm (these will be synchronized to WaveApps C# proxy adapter).
2. Modify `process_data` to implement desired calculation algorithm.

## Usage

```bash
python main.py <hostname> <wave_apps_port> <data_pub_port>
```

## Example

```bash
# Connect to a WaveApps proxy adapter running on localhost port 7198 and
# host data publication on port 7199. These ports must be unique for each
# Python calculation adapter.
python main.py localhost 7198 7199
```

The example python calculation will:
1. Connect to the specified WaveApps adapter (`<hostname>:<wave_apps_port>`)
2. Establish a publisher for calculated data (`<data_pub_port>`)
3. Subscribe to frequency measurements using filter: `FILTER ActiveMeasurements WHERE SignalType = 'FREQ'`
4. Receive incoming measurements, grouped by timestamp to the nearest second
5. Process grouped data in one-second windows
6. Calculate and display average frequency for each second of data
7. Trigger an event if frequency is out of range

## Key Features

- **Timestamp Grouping**: Groups measurements by whole seconds with subsecond alignment
- **Data Buffering**: Maintains 5-second measurement windows for processing
- **Time Validation**: Filters data based on configurable lag time (2 seconds) and lead time (2 seconds)
- **Downsampling Detection**: Tracks and reports when data rate exceeds processing capacity
- **Thread-Safe Processing**: Uses locks to ensure data integrity in multi-threaded environment

## Configuration

This example is configured with:
- Measurement window size: 5 seconds
- Samples per second: 3000
- Lag time: 2.0 seconds
- Lead time: 2.0 seconds

## What to Expect

When connected to a publisher, you should see:
- "Measurement reader established" message
- "Receiving measurements..." when data starts arriving
- Average frequency calculations for each second of data

Example output:
```
Receiving measurements...

Average frequency for 3000 values in second 45: 60.001234 Hz

Average frequency for 3000 values in second 46: 59.998765 Hz
```

## Data Processing

The example includes a `process_data` callback function that:
- Receives one-second buffers of time-aligned measurements
- Filters frequency values to reasonable range (59.95 to 60.05 Hz)
- Calculates average frequency across all valid measurements
- Demonstrates how to access measurement values, timestamps, and signal IDs
- Shows how to iterate through grouped measurement data

## Stopping the Calculation

Press `Enter` to gracefully disconnect and shut down the subscriber.
