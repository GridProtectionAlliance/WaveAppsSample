//******************************************************************************************************
//  PythonDataProxyBase.cs - Gbtc
//
//  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  01/09/2026 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************
// ReSharper disable SwitchStatementHandlesSomeKnownEnumValuesWithDefault
// ReSharper disable UnusedMember.Local

namespace WaveApps;

/// <summary>
/// Represents a base class for proxying data to and from a Python calculation adapter.
/// </summary>
/// <remarks>
/// This is internal functionality used to send and receive data from the Python adapter.
/// It is not expected that user will need to modify this code.
/// </remarks>
public abstract class PythonDataProxyBase : FacileActionAdapterBase
{
    #region [ Members ]
    
    // Nested Types
    private class ProxyDataPublisher(PythonDataProxyBase host) : DataPublisher
    {
        private DataSet? m_filteredDataSource;

        /// <inheritdoc />
        public override DataSet? DataSource
        {
            get => m_filteredDataSource;
            set
            {
                if (value is null)
                {
                    m_filteredDataSource = null;
                    return;
                }

                // Get configured input signal ID list from host adapter configuration
                string inputSignalIDs = host.GetInputSignalIDs();

                // Extract rows matching input signal IDs from active measurements in current metadata
                DataTable hostActiveMeasurements = value.Tables["ActiveMeasurements"]!;
                DataRow[] filteredRows = hostActiveMeasurements.Select($"SignalID IN ({inputSignalIDs})");

                // Create new filtered data source with same structure as original metadata, but only rows matching input signal IDs
                DataSet filteredDataSource = new(value.DataSetName);

                // Only adding ActiveMeasurements table to publisher metadata since that's the only key table needed
                filteredDataSource.Tables.Add(hostActiveMeasurements.Clone());
                DataTable filteredActiveMeasurements = filteredDataSource.Tables["ActiveMeasurements"]!;

                foreach (DataRow filteredRow in filteredRows)
                    filteredActiveMeasurements.ImportRow(filteredRow);

                m_filteredDataSource = filteredDataSource;
            }
        }

        /// <summary>
        /// Broadcasts user command response to all connected clients.
        /// </summary>
        /// <param name="response">Server response.</param>
        /// <param name="command">In response to command.</param>
        /// <param name="data">Data to return to client; null if none.</param>
        public void BroadcastCommandResponse(ServerResponse response, ServerCommand command, byte[]? data = null)
        {
            foreach (Guid clientID in ClientConnections.Keys)
                SendUserCommandResponse(clientID, response, command, data);
        }
        
        /// <summary>
        /// Sends user command response back to specified client with attached data.
        /// </summary>
        /// <param name="clientID">ID of client to send response.</param>
        /// <param name="response">Server response.</param>
        /// <param name="command">In response to command.</param>
        /// <param name="data">Data to return to client; null if none.</param>
        /// <returns><c>true</c> if send was successful; otherwise <c>false</c>.</returns>
        public bool SendUserCommandResponse(Guid clientID, ServerResponse response, ServerCommand command, byte[]? data = null)
        {
            const string Error = $"Response must range from '{nameof(ServerResponse.UserResponse00)}' to '{nameof(ServerResponse.UserResponse15)}'";

            return response is < ServerResponse.UserResponse00 or > ServerResponse.UserResponse15 ? 
                throw new ArgumentOutOfRangeException(nameof(response), response, Error) : 
                SendClientResponse(clientID, response, command, data);
        }
    }
    
    private class ProxyDataSubscriber : DataSubscriber
    {
        /// <summary>
        /// Occurs when metadata synchronization is complete.
        /// </summary>
        public event EventHandler? MetadataSyncComplete;
        
        /// <inheritdoc />
        public override DataSet? DataSource
        {
            get => base.DataSource;
            set
            {
                base.DataSource = value;

                // Notify Python calculation adapter of configuration change
                if (CommandChannelConnected)
                    SendServerCommand(ServerCommand.UserCommand00);
            }
        }

        /// <inheritdoc />
        protected override void OnConfigurationChanged()
        {
            base.OnConfigurationChanged();
            MetadataSyncComplete?.SafeInvoke(this, EventArgs.Empty);
        }
    }

    // Fields
    private ProxyDataPublisher? m_proxyDataPublisher;
    private ProxyDataSubscriber? m_proxyDataSubscriber;
    private Process? m_pythonProcess;
    private bool m_disposed;

    #endregion

    #region [ Properties ]
    
    /// <inheritdoc />
    public override DataSet? DataSource
    {
        get => base.DataSource;
        set
        {
            if (DataSetEqualityComparer.Default.Equals(DataSource, value))
                return;

            base.DataSource = value;

            if (m_proxyDataSubscriber is not null)
            {
                m_proxyDataSubscriber.DataSource = value;
                SynchronizeOutputMeasurements();
            }

            if (m_proxyDataPublisher is not null)
                m_proxyDataPublisher.DataSource = value;
        }
    }

    /// <inheritdoc />
    public override MeasurementKey[]? InputMeasurementKeys
    {
        get => base.InputMeasurementKeys;
        set
        {
            base.InputMeasurementKeys = value;

            if (value is null || m_proxyDataPublisher is null)
                return;

            m_proxyDataPublisher.MetadataTables = GetFilteredMetadataTables();
        }
    }

    /// <summary>
    /// Gets or sets the unique WaveApps host adapter publisher port.
    /// </summary>
    /// <remarks>
    /// Locally, this is the port the proxy publisher will use to listen for
    /// connections from the Python calculation adapter data subscriber.
    /// </remarks>
    [Description("Defines the unique host adapter publisher port")]
    [ConnectionStringParameter]
    public ushort HostAdapterPublisherPort { get; set; }

    /// <summary>
    /// Gets or sets the unique Python calculation adapter publisher port.
    /// </summary>
    /// <remarks>
    /// Locally, this is the port the proxy subscriber will use to connect to
    /// the Python calculation adapter data publisher.
    /// </remarks>
    [Description("Defines the unique Python calculation adapter publisher port")]
    [ConnectionStringParameter]
    public ushort PythonCalcPublisherPort { get; set; }
    
    /// <summary>
    /// Gets or sets flag that determines if Python calculation adapter will be automatically launched when host adapter is initialized.
    /// </summary>
    [Description("Defines flag that determines if Python calculation adapter will be automatically launched when host adapter is initialized.")]
    [ConnectionStringParameter]
    [DefaultValue(false)]
    public bool AutoLaunchPythonAdapter { get; set; }

    /// <summary>
    /// Gets or sets command line that will launch Python calculation adapter. Ensure absolute file path to main Python file is defined.
    /// </summary>
    [Description("Defines command line that will launch Python calculation adapter. Ensure absolute file path to main Python file is defined.")]
    [ConnectionStringParameter]
    [DefaultValue($"python -OO -X no_debug_ranges --disable-gil main.py localhost {{{nameof(HostAdapterPublisherPort)}}} {{{nameof(PythonCalcPublisherPort)}}}")]
    public string PythonLaunchCommand { get; set; } = null!;

    /// <inheritdoc />
    public override bool SupportsTemporalProcessing => false;

    /// <inheritdoc />
    public override string Status
    {
        get
        {
            StringBuilder status = new();

            status.AppendLine(base.Status);

            status.AppendLine($"         Host Adapter Port: {HostAdapterPublisherPort}");
            status.AppendLine($"  Python Calc Adapter Port: {PythonCalcPublisherPort}");
            status.AppendLine($"Auto-Launch Python Adapter: {AutoLaunchPythonAdapter}");
            status.AppendLine($"     Python Launch Command: {PythonLaunchCommand}");

            if (m_proxyDataPublisher is not null)
            {
                status.AppendLine();
                status.AppendLine("--------------------------");
                status.AppendLine("  Proxy Publisher Status  ");
                status.AppendLine("--------------------------");
                status.AppendLine();
                status.AppendLine(m_proxyDataPublisher.Status);
            }

            if (m_proxyDataSubscriber is not null)
            {
                status.AppendLine();
                status.AppendLine("---------------------------");
                status.AppendLine("  Proxy Subscriber Status  ");
                status.AppendLine("---------------------------");
                status.AppendLine();
                status.AppendLine(m_proxyDataSubscriber.Status);
            }

            return status.ToString();
        }
    }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Releases the unmanaged resources used by the <see cref="PythonDataProxyBase"/> object and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        if (m_disposed)
            return;

        try
        {
            if (!disposing)
                return;

            if (m_pythonProcess is not null)
            {
                m_pythonProcess.CancelOutputRead();
                m_pythonProcess.CancelErrorRead();

                m_pythonProcess.Exited -= m_pythonProcess_Exited;
                m_pythonProcess.OutputDataReceived -= m_pythonProcess_OutputDataReceived;
                m_pythonProcess.ErrorDataReceived -= m_pythonProcess_ErrorDataReceived;
                
                // TODO: Consider sending termination signal (custom command) to Python adapter
                m_pythonProcess.Kill();
                m_pythonProcess.Close();

            }

            if (m_proxyDataPublisher is not null)
            {
                m_proxyDataPublisher.StatusMessage -= m_proxyDataPublisher_StatusMessage;
                m_proxyDataPublisher.ProcessException -= m_proxyDataPublisher_ProcessException;
                m_proxyDataPublisher.ClientConnected -= m_proxyDataPublisher_ClientConnected;
                m_proxyDataPublisher.Dispose();
            }

            m_proxyDataPublisher = null;

            if (m_proxyDataSubscriber is not null)
            {
                m_proxyDataSubscriber.StatusMessage -= m_proxyDataSubscriber_StatusMessage;
                m_proxyDataSubscriber.ProcessException -= m_proxyDataSubscriber_ProcessException;
                m_proxyDataSubscriber.ConnectionEstablished -= m_proxyDataSubscriber_ConnectionEstablished;
                m_proxyDataSubscriber.ConnectionTerminated -= m_proxyDataSubscriber_ConnectionTerminated;
                m_proxyDataSubscriber.NewMeasurements -= m_proxyDataSubscriber_NewMeasurements;
                m_proxyDataSubscriber.MetaDataReceived -= m_proxyDataSubscriber_MetaDataReceived;
                m_proxyDataSubscriber.ReceivedUserCommandResponse -= m_proxyDataSubscriber_ReceivedUserCommandResponse;
                m_proxyDataSubscriber?.Dispose();
            }

            m_proxyDataSubscriber = null;
        }
        finally
        {
            m_disposed = true;          // Prevent duplicate dispose.
            base.Dispose(disposing);    // Call base class Dispose().
        }
    }

    /// <inheritdoc />
    public override void Initialize()
    {
        new ConnectionStringParser().ParseConnectionString(ConnectionString, this);

        if (HostAdapterPublisherPort == 0)
            throw new ArgumentOutOfRangeException(nameof(HostAdapterPublisherPort), $"Port must be in range of 1 to {ushort.MaxValue}");

        if (PythonCalcPublisherPort == 0)
            throw new ArgumentOutOfRangeException(nameof(PythonCalcPublisherPort), $"Port must be in range of 1 to {ushort.MaxValue}");

        base.Initialize();

        // Make sure a device record exists for this adapter to associate measurements with
        using AdoDataConnection connection = new(ConfigSettings.Instance);
        TableOperations<Device> deviceTable = new(connection);

        string deviceAcronym = $"{Name}_DATA-SYNC";
        Device? deviceRecord = deviceTable.QueryRecordWhere("Acronym = {0}", deviceAcronym);

        if (deviceRecord is null)
        {
            TableOperations<Historian> historianTable = new(connection);
            Historian? primaryHistorian = historianTable.QueryRecordWhere("IsPrimary <> 0");

            deviceRecord = deviceTable.NewRecord();
            Debug.Assert(deviceRecord is not null);

            deviceRecord.Acronym = deviceAcronym;
            deviceRecord.Name = $"{Name} Python Data Proxy Adapter Host Synchronization Device";
            deviceRecord.IsConcentrator = true;
            deviceRecord.HistorianID = primaryHistorian?.ID;
            deviceRecord.ConnectionString = "protocol=VirtualInput";
            deviceRecord.Description = "Python data proxy adapter host synchronization device for associated incoming measurements";
            deviceRecord.LoadOrder = 9999;
            deviceRecord.Enabled = true;

            deviceTable.AddNewRecord(deviceRecord);

            // Requery again to get record with assigned ID
            deviceRecord = deviceTable.QueryRecordWhere("Acronym = {0}", deviceAcronym);
            Debug.Assert(deviceRecord is not null);
        }

        // Get runtime ID of device record
        TableOperations<Runtime> runtimeTable = new(connection);
        Runtime? runtimeRecord = runtimeTable.QueryRecordWhere("SourceTable = 'Device' AND SourceID = {0}", deviceRecord.ID);
        Debug.Assert(runtimeRecord is not null);

        m_proxyDataPublisher = new ProxyDataPublisher(this); // Initialize with HostAdapterPublisherPort

        // Attach to publisher events
        m_proxyDataPublisher.StatusMessage += m_proxyDataPublisher_StatusMessage;
        m_proxyDataPublisher.ProcessException += m_proxyDataPublisher_ProcessException;
        m_proxyDataPublisher.ClientConnected += m_proxyDataPublisher_ClientConnected;

        m_proxyDataPublisher.DataSource = DataSource;
        m_proxyDataPublisher.Name = $"{Name}_PROXY-DATA-PUBLISHER";
        m_proxyDataPublisher.ID = (uint)runtimeRecord.ID;
        m_proxyDataPublisher.UseBaseTimeOffsets = true;
        m_proxyDataPublisher.AllowPayloadCompression = true;
        m_proxyDataPublisher.MetadataTables = GetFilteredMetadataTables();
        m_proxyDataPublisher.ConnectionString = $"commandChannel={{port={HostAdapterPublisherPort}}}";
        m_proxyDataPublisher.Initialize();

        // Start publisher
        m_proxyDataPublisher.Start();

        m_proxyDataSubscriber = new ProxyDataSubscriber(); // Initialize with PythonCalcPublisherPort

        // Attach to subscriber events
        m_proxyDataSubscriber.StatusMessage += m_proxyDataSubscriber_StatusMessage;
        m_proxyDataSubscriber.ProcessException += m_proxyDataSubscriber_ProcessException;
        m_proxyDataSubscriber.ConnectionEstablished += m_proxyDataSubscriber_ConnectionEstablished;
        m_proxyDataSubscriber.ConnectionTerminated += m_proxyDataSubscriber_ConnectionTerminated;
        m_proxyDataSubscriber.NewMeasurements += m_proxyDataSubscriber_NewMeasurements;
        m_proxyDataSubscriber.MetaDataReceived += m_proxyDataSubscriber_MetaDataReceived;
        m_proxyDataSubscriber.MetadataSyncComplete += m_proxyDataSubscriber_MetadataSyncComplete;
        m_proxyDataSubscriber.ReceivedUserCommandResponse += m_proxyDataSubscriber_ReceivedUserCommandResponse;

        m_proxyDataSubscriber.DataSource = DataSource;
        m_proxyDataSubscriber.Name = $"{Name}_PROXY-DATA-SUBSCRIBER";
        m_proxyDataSubscriber.ID = (uint)runtimeRecord.ID;
        m_proxyDataSubscriber.ConnectionString = 
            $$"""
              server=localhost:{{PythonCalcPublisherPort}}; 
              interface=0.0.0.0; 
              autoConnect=true; 
              autoSynchronizeMetadata=true; 
              compression=true; 
              internal=true; 
              useSourcePrefixNames=false; 
              securityMode=None; 
              outputMeasurements={FILTER ActiveMeasurements WHERE True}; 
              receiveInternalMetadata=true; 
              receiveExternalMetadata=true
              """;
        
        m_proxyDataSubscriber.Initialize();

        // Start subscriber
        m_proxyDataSubscriber.Start();

        // Automatically launch Python calculation adapter when configured to do so
        if (!AutoLaunchPythonAdapter)
            return;

        if (string.IsNullOrWhiteSpace(PythonLaunchCommand))
            throw new ArgumentException($"{nameof(PythonLaunchCommand)} is not defined, cannot launch Python calculation adapter");
        
        string[] args = PythonLaunchCommand.Split(' ');

        if (args.Length < 2)
            throw new ArgumentException($"{nameof(PythonLaunchCommand)} has no defined arguments, cannot launch Python calculation adapter");

        string pythonExe = args[0];
        string argumentList = string.Join(' ', args[1..])
            .Replace($"{{{nameof(HostAdapterPublisherPort)}}}", HostAdapterPublisherPort.ToString())
            .Replace($"{{{nameof(PythonCalcPublisherPort)}}}", PythonCalcPublisherPort.ToString());

        ProcessStartInfo startInfo = new(pythonExe, argumentList)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            WindowStyle = ProcessWindowStyle.Hidden,
            ErrorDialog = false,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        m_pythonProcess = new Process();
        m_pythonProcess.StartInfo = startInfo;
        m_pythonProcess.EnableRaisingEvents = true;
        m_pythonProcess.Exited += m_pythonProcess_Exited;
        m_pythonProcess.OutputDataReceived += m_pythonProcess_OutputDataReceived;
        m_pythonProcess.ErrorDataReceived += m_pythonProcess_ErrorDataReceived;
        
        m_pythonProcess.Start();
        
        m_pythonProcess.BeginOutputReadLine();
        m_pythonProcess.BeginErrorReadLine();
    }

    /// <inheritdoc />
    public override string GetShortStatus(int maxLength)
    {
        return $"Published {m_proxyDataPublisher?.ProcessedMeasurements:N0} and received {m_proxyDataSubscriber?.ProcessedMeasurements:N0} measurements so far...";
    }

    public override void QueueMeasurementsForProcessing(IEnumerable<IMeasurement> measurements)
    {
        // Take measurements received from host and send to Python calculation adapter via proxy publisher
        m_proxyDataPublisher?.QueueMeasurementsForProcessing(measurements);
    }

    // Get configured input measurement keys that define measurements to be published to Python adapter
    private string GetInputSignalIDs()
    {
        return InputMeasurementKeys is { Length: > 0 } ?
            string.Join(',', InputMeasurementKeys.Select(key => $"'{key.SignalID:D}'")) :
            $"'{Guid.Empty}'";
    }

    private string GetFilteredMetadataTables()
    {
        string inputSignalIDs = GetInputSignalIDs();

        // Filter metadata to be published down to these inputs for simplicity and optimal minimal metadata transmission
        return $"""
                SELECT UniqueID, OriginalSource, IsConcentrator, Acronym, Name, AccessID, ParentAcronym, CompanyAcronym, VendorAcronym, VendorDeviceName, Longitude, Latitude, InterconnectionName, ContactList, Enabled, UpdatedOn
                  FROM DeviceDetail
                  WHERE IsConcentrator = 0 AND EXISTS (
                    SELECT 1
                      FROM MeasurementDetail
                      WHERE MeasurementDetail.DeviceAcronym = DeviceDetail.Acronym AND 
                            MeasurementDetail.SignalID IN ({inputSignalIDs}));
                SELECT DeviceAcronym, ID, SignalID, PointTag, AlternateTag, SignalReference, SignalAcronym, PhasorSourceIndex, Description, Internal, Enabled, UpdatedOn
                  FROM MeasurementDetail
                  WHERE SignalID IN ({inputSignalIDs});
                SELECT ID, DeviceAcronym, Label, Type, Phase, PrimaryVoltageID, SecondaryVoltageID, SourceIndex, BaseKV, UpdatedOn
                  FROM PhasorDetail
                  WHERE EXISTS (
                    SELECT 1
                      FROM MeasurementDetail
                      WHERE MeasurementDetail.DeviceAcronym = PhasorDetail.DeviceAcronym AND
                            MeasurementDetail.SignalID IN ({inputSignalIDs}));
                SELECT TOP 1 Version AS VersionNumber
                  FROM VersionInfo AS SchemaVersion
                """;
    }

    private void SynchronizeOutputMeasurements()
    {
        // Reapply output measurements if reinitializing - this way filter expressions and/or sourceIDs
        // will be reapplied. This can be important after a meta-data refresh which may have added new
        // measurements that could now be applicable as desired output measurements.
        OutputMeasurements = m_proxyDataSubscriber?.OutputMeasurements;

        #pragma warning disable CA2245
        OutputSourceIDs = OutputSourceIDs;
        #pragma warning restore CA2245
    }

    private void m_proxyDataPublisher_StatusMessage(object? sender, EventArgs<string> e)
    {
        OnStatusMessage(MessageLevel.Info, $"[Data Proxy Publisher]: {e.Argument}", nameof(m_proxyDataPublisher_StatusMessage));
    }

    private void m_proxyDataPublisher_ProcessException(object? sender, EventArgs<Exception> e)
    {
        OnProcessException(MessageLevel.Info, e.Argument, nameof(m_proxyDataSubscriber_ProcessException));
    }

    private void m_proxyDataPublisher_ClientConnected(object? sender, EventArgs<Guid, string, string> e)
    {
        Guid clientID = e.Argument1;
        string connectionID = e.Argument2;
        string subscriberInfo = e.Argument3;

        OnStatusMessage(MessageLevel.Info, $"[Data Proxy Publisher]: Client \"{connectionID}\" connected: {subscriberInfo}");

        // Serialize adapter properties with 'AmbientValueAttribute' into key-value pair string for sending to Python adapter
        string connectionString = GetType()
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property is { CanRead: true, CanWrite: true })
            .Select<PropertyInfo, (string, string)?>(property =>
            {
                if (!property.TryGetAttributes(out AmbientValueAttribute[]? attributes) || attributes!.Length == 0)
                    return null;

                object? ambientValue = attributes[0].Value;
                return ambientValue is null ? null : ($"{ambientValue}", $"{property.GetValue(this)}");
            })
            .OfType<(string key, string value)>()
            .ToDictionary(kvp => kvp.key, kvp => kvp.value)
            .JoinKeyValuePairs();

        // Send serialized property values to Python adapter, using user response 2
        bool success = m_proxyDataPublisher?.SendUserCommandResponse(clientID, ServerResponse.UserResponse02, ServerCommand.UserCommand02, Encoding.UTF8.GetBytes(connectionString)) ?? false;
        
        if (success)
            OnStatusMessage(MessageLevel.Info, $"[Data Proxy Publisher]: Successfully sent serialization of adapter properties to Python calculation adapter \"{connectionID}\".");
        else
            OnStatusMessage(MessageLevel.Error, $"[Data Proxy Publisher]: Failed to send serialization of adapter properties to Python calculation adapter \"{connectionID}\".");
    }

    private void m_proxyDataSubscriber_StatusMessage(object? sender, EventArgs<string> e)
    {
        OnStatusMessage(MessageLevel.Info, $"[Python Proxy Subscriber]: {e.Argument}", nameof(m_proxyDataSubscriber_StatusMessage));
    }

    private void m_proxyDataSubscriber_ProcessException(object? sender, EventArgs<Exception> e)
    {
        OnProcessException(MessageLevel.Error, e.Argument, nameof(m_proxyDataSubscriber_ProcessException));
    }

    private void m_proxyDataSubscriber_ConnectionEstablished(object? sender, EventArgs e)
    {
        OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Connection established, synchronizing outputs with host...");
        SynchronizeOutputMeasurements();
    }

    private void m_proxyDataSubscriber_ConnectionTerminated(object? sender, EventArgs e)
    {
        OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Connection terminated");
    }

    private void m_proxyDataSubscriber_NewMeasurements(object? sender, EventArgs<ICollection<IMeasurement>> e)
    {
        // Forward measurements received from Python calculation adapter to host
        OnNewMeasurements(e.Argument);
    }

    private void m_proxyDataSubscriber_MetaDataReceived(object? sender, EventArgs<DataSet> e)
    {
        OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Received metadata from Python calculation adapter");
    }

    private void m_proxyDataSubscriber_MetadataSyncComplete(object? sender, EventArgs e)
    {
        OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Metadata synchronization from Python calculation adapter complete");
        OnConfigurationChanged();
    }

    private void m_proxyDataSubscriber_ReceivedUserCommandResponse(object? sender, DataSubscriber.UserCommandArgs e)
    {
        ServerResponse response = e.Response;
        ServerCommand command = e.Command;
        byte[] buffer = e.Buffer;
        int startIndex = e.StartIndex;
        int length = e.Length;
        
        switch (response)
        {
            case ServerResponse.UserResponse00 when command == ServerCommand.UserCommand00:
                OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Received configuration changed notification from Python calculation adapter");
                m_proxyDataSubscriber?.RefreshMetadata();
                break;
            // The 'UserResponse02' used for sending serialized property values to Python adapter is on a different
            // communication channel (proxy publisher), so we could have used 'UserResponse02' here for processing
            // event publication requests from Python calculation adapter. However, we instead chose the distinct
            // 'UserResponse03' for clarity and to reduce confusion when reviewing the code.
            case ServerResponse.UserResponse03 when command == ServerCommand.UserCommand03:
            {
                OnStatusMessage(MessageLevel.Info, $"[Python Proxy Subscriber]: Processing {length:N0}-byte event publication request from Python calculation adapter");

                string connectionString = Encoding.UTF8.GetString(buffer, startIndex, length);
                Dictionary<string, string> settings = connectionString.ParseKeyValuePairs();

                if (!settings.TryGetValue("SignalID", out string? setting) || !Guid.TryParse(setting, out Guid signalID))
                {
                    OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event publication request, failed to parse 'SignalID' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("EventID", out setting) || !Guid.TryParse(setting, out Guid eventID))
                {
                    OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event publication request, failed to parse 'EventID' parameter");
                    return;
                }

                if (!settings.TryGetValue("Timestamp", out setting) || !Ticks.TryParse(setting, out Ticks timestamp))
                {
                    OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event publication request, failed to parse 'Timestamp' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("AlarmTimestamp", out setting) || !Ticks.TryParse(setting, out Ticks alarmTimestamp))
                {
                    OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event publication request, failed to parse 'AlarmTimestamp' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("Value", out setting) || !double.TryParse(setting, out double value))
                {
                    OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event publication request, failed to parse 'Value' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("EventDetails", out string? eventDetails))
                {
                    OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event publication request, failed to parse 'EventDetails' parameter");
                    return;
                }

                MeasurementKey alarmKey = MeasurementKey.LookUpBySignalID(signalID);

                if (alarmKey == MeasurementKey.Undefined)
                {
                    OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Failed to process Python event publication request, cannot find measurement key for specified 'SignalID'");
                    return;
                }

                AlarmMeasurement alarmMeasurement = new()
                {
                    Timestamp = timestamp,
                    AlarmTimestamp = alarmTimestamp,
                    Value = value,
                    AlarmID = signalID,
                    Metadata = alarmKey.Metadata
                };
                
                using AdoDataConnection connection = new(ConfigSettings.Instance);
                TableOperations<EventDetails> tableOperations = new(connection);

                if (value > 0)
                {
                    // Start of event
                    EventDetails record = new()
                    {
                        StartTime = alarmTimestamp,
                        EndTime = DateTime.MinValue,
                        EventGuid = eventID,
                        Type = "PythonProxyCalc",
                        MeasurementID = signalID,
                        Details = eventDetails
                    };

                    tableOperations.AddNewRecord(record);
                }
                else
                {
                    // End of event
                    EventDetails? record = tableOperations.QueryRecordWhere("EventGuid = {0}", eventID);

                    if (record is null)
                    {
                        OnStatusMessage(MessageLevel.Error, $"[Python Proxy Subscriber]: Failed to find existing event record \"{eventID}\" to update end of event");
                        return;
                    }

                    record.EndTime = alarmTimestamp;
                    tableOperations.UpdateRecord(record);
                }

                OnNewMeasurements([alarmMeasurement]);
                break;
            }
            default:
                OnStatusMessage(MessageLevel.Warning, $"[Python Proxy Subscriber]: Received unhandled {length:N0}-byte user server response {response} for command {command} from Python calculation adapter");
                break;
        }
    }

    private void m_pythonProcess_Exited(object? sender, EventArgs e)
    {
        OnStatusMessage(MessageLevel.Warning, "[Python Calculation Adapter]: python process exited unexpectedly");

        // TODO:
        // Consider implementing auto-restart logic with configurable delay and a limit on number of restart attempts within a certain
        // time frame to prevent infinite restart loops in case of persistent errors causing Python adapter to crash on startup
    }

    private void m_pythonProcess_OutputDataReceived(object sender, DataReceivedEventArgs e)
    {
        if (e.Data is not null)
            OnStatusMessage(MessageLevel.Info, $"[Python Calculation Adapter]: {e.Data}");
    }

    private void m_pythonProcess_ErrorDataReceived(object sender, DataReceivedEventArgs e)
    {
        if (e.Data is not null)
            OnStatusMessage(MessageLevel.Error, $"[Python Calculation Adapter]: {e.Data}");
    }

    #endregion
}