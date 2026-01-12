# WaveApps C# Host Adapter Example

This example defines a WaveApps C# host adapter that sends data to and from the Python calculation adapter.

The main task for the user here is to define any custom properties specific to the Python calculation algorithm configuration.

## Key User Tasks
1. Rename project (`WaveAppsHostAdapter`) and proxy adapter from (`MyCalcDataProxyAdapter`) to name more relevant to algorithm intent. For example, if calculation algorithm intent is to calculate average frequency, rename project file to `AvgFreqHostAdapter` then adapter file and class to `AvgFreqProxyAdapter`.
2. Add custom properties specific to the Python calculation algorithm configuration to proxy adapter. These values will be sent to Python adapter when algorithm is started.

