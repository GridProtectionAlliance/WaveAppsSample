# WaveApps C# Host Adapter Example

This example defines a WaveApps C# host adapter that sends data to and from the Python calculation adapter.

The main task for the user here is to define any custom properties specific to the Python calculation algorithm configuration. The purpose of this adapter is to define UI configuration for the calculation in the WaveApps host application, to send the configuration values to the Python adapter when the algorithm is started, and handle data exchange between the host and calculation adapters.

## Key User Tasks
1. Rename project (`WaveAppsHostAdapter`) and proxy adapter from (`AvgFreqCalcAdapter`) to name more relevant to algorithm intent. For example, if calculation algorithm intent is to calculate power, e.g., (MW, MVA, MVAR), rename project file to `PowerCalcAdapter` then adapter file and class to `PowerCalcAdapter`.
2. Add custom properties specific to the Python calculation algorithm configuration to proxy adapter. These configuration values will be sent to Python adapter when algorithm is started.

