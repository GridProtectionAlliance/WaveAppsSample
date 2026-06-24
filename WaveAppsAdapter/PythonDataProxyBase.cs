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
using Gemstone.ComponentModel.DataAnnotations;
using Gemstone.IO;
using System.ComponentModel.DataAnnotations;

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
    private bool m_awaitingHostMetadataSync;
    private bool m_disposed;

    // Diagnostics: measurement-to-publisher route latency. Only updated when
    // LogRouteCalculationLatency is enabled; otherwise these stay at 0 and incur no cost.
    // Tracks the most recent QueueMeasurementsForProcessing call so that, when the
    // underlying DataPublisher emits "Starting measurement route calculation...", we
    // can report how long the measurement sat between OnNewMeasurements and the
    // publisher's TSSC encoder pipeline. Useful when diagnosing dropped or delayed
    // single measurements (e.g., events/alarms) that the routing tables may batch.
    private long m_lastQueueTicks;
    private int m_lastQueueCount;

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
                m_proxyDataSubscriber.DataSource = value;

            if (m_proxyDataPublisher is not null)
                m_proxyDataPublisher.DataSource = value;

            if (!m_awaitingHostMetadataSync)
                return;

            m_awaitingHostMetadataSync = false;

            // Synchronize output measurements with Python calculation adapter outputs after
            // host metadata has been refreshed following notification from subscriber, this
            // means any measurements defined by Python calculation adapter as outputs are
            // also now available in host metadata and can be selected as outputs
            SynchronizeOutputMeasurements();
        }
    }

    /// <summary>
    /// Property hidden - not used by <see cref="PythonDataProxyBase"/>.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
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

    /// <inheritdoc />
    [EditorBrowsable(EditorBrowsableState.Never)] // Autoconfigured based on Python calculation adapter configuration, so hide from UI
    public override IMeasurement[]? OutputMeasurements
    {
        get => base.OutputMeasurements;
        set => base.OutputMeasurements = value;
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
    [DefaultValue(65510)]
    [Range(1, ushort.MaxValue)]
    [Label("Host Adapter Publisher Port")]
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
    [DefaultValue(65515)]
    [Range(1, ushort.MaxValue)]
    [Label("Python Publisher Port")]
    public ushort PythonCalcPublisherPort { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if Python calculation adapter will be automatically launched when host adapter is initialized.
    /// </summary>
    [Description("Defines flag that determines if Python calculation adapter will be automatically launched when host adapter is initialized.")]
    [ConnectionStringParameter]
    [DefaultValue(false)]
    [Label("Auto-Launch Python Adapter")]
    public bool AutoLaunchPythonAdapter { get; set; }

    /// <summary>
    /// Gets or sets command line that will launch Python calculation adapter. Ensure absolute file path to main Python file is defined.
    /// </summary>
    [Description("Defines command line that will launch Python calculation adapter. Ensure absolute file path to main Python file is defined.")]
    [ConnectionStringParameter]
    [DefaultValue($"python -OO -X no_debug_ranges --disable-gil main.py localhost {{{nameof(HostAdapterPublisherPort)}}} {{{nameof(PythonCalcPublisherPort)}}}")]
    [Label("Python Launch Command")]
    public string PythonLaunchCommand { get; set; } = null!;

    /// <summary>
    /// Gets or sets flag that enables logging of route-calculation latency for diagnostic purposes.
    /// </summary>
    /// <remarks>
    /// When enabled, <see cref="QueueMeasurementsForProcessing"/> stamps the time and count of each
    /// inbound measurement batch, and the publisher's status-message handler reports the elapsed
    /// time when the underlying <c>DataPublisher</c> emits "Starting measurement route calculation...".
    /// Useful for diagnosing dropped or delayed single measurements (e.g., events/alarms) that may be
    /// batched by the routing tables. Off by default - the timing/materialization cost is unwanted in
    /// hot paths (e.g., point-on-wave measurement streaming).
    /// </remarks>
    [Description("Defines flag that enables logging of route-calculation latency for diagnostic purposes. Adds a small per-batch cost to QueueMeasurementsForProcessing - leave disabled in production hot paths (e.g., point-on-wave streaming).")]
    [ConnectionStringParameter]
    [DefaultValue(false)]
    [Label("Log Route Calculation Latency")]
    public bool LogRouteCalculationLatency { get; set; }

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

        base.Initialize();

        if (HostAdapterPublisherPort == 0)
            throw new ArgumentOutOfRangeException(nameof(HostAdapterPublisherPort), $"Port must be in range of 1 to {ushort.MaxValue}");

        if (PythonCalcPublisherPort == 0)
            throw new ArgumentOutOfRangeException(nameof(PythonCalcPublisherPort), $"Port must be in range of 1 to {ushort.MaxValue}");

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

        // TSSC requirement: per-subscriber the publisher only encodes with TSSC when BOTH
        // AllowPayloadCompression == true here AND the subscriber's request includes
        // OperationalModes.CompressPayloadData (i.e., the Python adapter's connection
        // string sets compression=true). When either is missing, the wire format falls
        // back to the uncompressed CompactMeasurement encoding. TSSC is preferred for
        // streaming measurements - keep this flag true unless intentionally disabling.
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
        // Hot path - when route-latency diagnostics are off, just forward the enumerable
        // unchanged so we incur no materialization or timing cost. This matters for
        // high-rate streams (e.g., point-on-wave).
        if (!LogRouteCalculationLatency)
        {
            m_proxyDataPublisher?.QueueMeasurementsForProcessing(measurements);
            return;
        }

        // Diagnostic path - materialize once so we can record an accurate count without
        // re-enumerating the source, then stamp the queue time so the route-calculation
        // watcher (in m_proxyDataPublisher_StatusMessage) can report end-to-end latency
        // between OnNewMeasurements and the publisher's route activity.
        IList<IMeasurement> queued = measurements as IList<IMeasurement> ?? measurements.ToList();

        Interlocked.Exchange(ref m_lastQueueTicks, DateTime.UtcNow.Ticks);
        Interlocked.Exchange(ref m_lastQueueCount, queued.Count);

        m_proxyDataPublisher?.QueueMeasurementsForProcessing(queued);
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
        LoadOutputSourceIDs(this);
    }

    // --- Proxy Data Publisher Event Handlers ---

    private void m_proxyDataPublisher_StatusMessage(object? sender, EventArgs<UILogMessage> e)
    {
        string message = e.Argument.Message;

        // Route-calculation latency watcher (gated on LogRouteCalculationLatency): when the
        // underlying DataPublisher emits "Starting measurement route calculation...", report
        // how long ago the most recent QueueMeasurementsForProcessing call was. A high latency
        // here suggests the routing tables are batching measurements before they reach the
        // TSSC encoder.
        if (LogRouteCalculationLatency && message.StartsWith("Starting measurement route calculation", StringComparison.Ordinal))
        {
            long lastQueue = Interlocked.Read(ref m_lastQueueTicks);

            if (lastQueue > 0)
            {
                long deltaTicks = DateTime.UtcNow.Ticks - lastQueue;
                double deltaMs = deltaTicks / (double)Ticks.PerMillisecond;
                int queuedCount = m_lastQueueCount;

                OnStatusMessage(
                    MessageLevel.Info,
                    $"[Data Proxy Publisher] [TIMING]: Route calculation started {deltaMs:F2} ms after most recent QueueMeasurementsForProcessing ({queuedCount:N0} measurement{(queuedCount == 1 ? "" : "s")} queued).",
                    nameof(m_proxyDataPublisher_StatusMessage));
            }
            else
            {
                OnStatusMessage(
                    MessageLevel.Info,
                    "[Data Proxy Publisher] [TIMING]: Route calculation started before any measurements were queued (initial subscription/recalc).",
                    nameof(m_proxyDataPublisher_StatusMessage));
            }
        }

        OnStatusMessage(MessageLevel.Info, $"[Data Proxy Publisher]: {message}", nameof(m_proxyDataPublisher_StatusMessage));
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
                object? propertyValue = property.GetValue(this);

                return propertyValue switch
                {
                    MeasurementKey key => ($"{ambientValue}", key.SignalID.ToString("D")),
                    MeasurementKey[] keys => ($"{ambientValue}", string.Join(',', keys.Select(key => key.SignalID.ToString("D")))),
                    IMeasurement measurement => ($"{ambientValue}", $"{measurement.Key.SignalID:D}"),
                    IMeasurement[] measurementArray => ($"{ambientValue}", string.Join(',', measurementArray.Select(m => m.Key.SignalID.ToString("D")))),
                    _ => ambientValue is null ? null : ($"{ambientValue}", $"{propertyValue}")
                };
            })
            .OfType<(string key, string value)>() // Gets non-null ambient key-value tuples
            .ToDictionary(kvp => kvp.key, kvp => kvp.value)
            .JoinKeyValuePairs();

        // Send serialized property values to Python adapter, using user response 2
        bool success = m_proxyDataPublisher?.SendUserCommandResponse(clientID, ServerResponse.UserResponse02, ServerCommand.UserCommand02, Encoding.UTF8.GetBytes(connectionString)) ?? false;

        if (success)
            OnStatusMessage(MessageLevel.Info, $"[Data Proxy Publisher]: Successfully sent serialization of adapter properties to Python calculation adapter \"{connectionID}\".");
        else
            OnStatusMessage(MessageLevel.Error, $"[Data Proxy Publisher]: Failed to send serialization of adapter properties to Python calculation adapter \"{connectionID}\".");
    }

    // --- Proxy Data Subscriber Event Handlers ---

    private void m_proxyDataSubscriber_StatusMessage(object? sender, EventArgs<UILogMessage> e)
    {
        OnStatusMessage(MessageLevel.Info, $"[Python Proxy Subscriber]: {e.Argument.Message}", nameof(m_proxyDataSubscriber_StatusMessage));
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
        // Most measurements flow straight through to the host. BufferBlock measurements are the
        // STTP transport for event publications from the Python calculation adapter (replacing the
        // legacy `UserResponse03` connection-string flow): each carries a UTF-8 JSON event payload
        // that we parse into an AlarmMeasurement + EventDetails row before forwarding.
        List<IMeasurement>? eventAlarms = null;
        List<IMeasurement>? regularMeasurements = null;

        foreach (IMeasurement measurement in e.Argument)
        {
            if (measurement is BufferBlockMeasurement { Buffer: not null, Length: > 0 } bufferBlock)
            {
                AlarmMeasurement? alarm = ProcessEventBufferBlock(bufferBlock);

                if (alarm is not null)
                {
                    eventAlarms ??= [];
                    eventAlarms.Add(alarm);
                }
            }
            else
            {
                regularMeasurements ??= [];
                regularMeasurements.Add(measurement);
            }
        }

        if (regularMeasurements is not null)
            OnNewMeasurements(regularMeasurements);

        if (eventAlarms is not null)
            OnNewMeasurements(eventAlarms);
    }

    /// <summary>
    /// Parses a buffer-block event published by the Python calculation adapter and converts it to
    /// an <see cref="AlarmMeasurement"/>, also persisting the start / end of event into the host
    /// <c>EventDetails</c> table. Mirrors the JSON schema emitted by
    /// <c>python-calc-adapter/data_proxy.py::publish_event</c>, which is intentionally identical
    /// to the substation-to-central schema in
    /// <c>openHistorian/waveAppsDataTransfer/DataPublisher.cs::SendEventPublication</c> so a
    /// single receiver implementation can decode events from either source.
    /// </summary>
    /// <remarks>
    /// The buffer-block payload is a UTF-8 JSON document with fields <c>EventID</c>, <c>Type</c>,
    /// <c>StartTime</c>, <c>EndTime</c>, <c>Value</c>, and <c>EventDetails</c>. The signal the
    /// event is associated with is implicit in the buffer block frame's SIGNAL INDEX header and
    /// surfaces as <see cref="IMeasurement.Key"/>.<c>SignalID</c> on the measurement instance.
    /// </remarks>
    private AlarmMeasurement? ProcessEventBufferBlock(BufferBlockMeasurement bufferBlock)
    {
        Guid signalID = bufferBlock.Key.SignalID;

        OnStatusMessage(MessageLevel.Info, $"[Python Proxy Subscriber]: Processing {bufferBlock.Length:N0}-byte event buffer-block from Python calculation adapter for signal {signalID:D}");

        Guid eventID;
        string eventType;
        long startTimeTicks;
        long endTimeTicks;
        double value;
        string eventDetails;

        try
        {
            // Parse the JSON payload. Use JsonDocument so we can be tolerant of missing optional
            // fields and report exactly which property failed without throwing on the first miss.
            using JsonDocument document = JsonDocument.Parse(new ReadOnlyMemory<byte>(bufferBlock.Buffer, 0, bufferBlock.Length));
            JsonElement root = document.RootElement;

            if (!root.TryGetProperty("EventID", out JsonElement eventIDElement) || !Guid.TryParse(eventIDElement.GetString(), out eventID))
            {
                OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event buffer-block, failed to parse 'EventID' field");
                return null;
            }

            if (!root.TryGetProperty("Type", out JsonElement typeElement) || string.IsNullOrWhiteSpace(eventType = typeElement.GetString() ?? string.Empty))
            {
                OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event buffer-block, failed to parse 'Type' field");
                return null;
            }

            if (!root.TryGetProperty("StartTime", out JsonElement startTimeElement) || !startTimeElement.TryGetInt64(out startTimeTicks))
            {
                OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event buffer-block, failed to parse 'StartTime' field");
                return null;
            }

            if (!root.TryGetProperty("EndTime", out JsonElement endTimeElement) || !endTimeElement.TryGetInt64(out endTimeTicks))
            {
                OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event buffer-block, failed to parse 'EndTime' field");
                return null;
            }

            if (!root.TryGetProperty("Value", out JsonElement valueElement) || !valueElement.TryGetDouble(out value))
            {
                OnStatusMessage(MessageLevel.Error, "[Python Proxy Subscriber]: Cannot process Python event buffer-block, failed to parse 'Value' field");
                return null;
            }

            // EventDetails is optional - missing or null is treated as empty.
            eventDetails = root.TryGetProperty("EventDetails", out JsonElement eventDetailsElement) && eventDetailsElement.ValueKind == JsonValueKind.String
                ? eventDetailsElement.GetString() ?? string.Empty
                : string.Empty;
        }
        catch (JsonException ex)
        {
            OnStatusMessage(MessageLevel.Error, $"[Python Proxy Subscriber]: Failed to parse event buffer-block JSON payload: {ex.Message}");
            return null;
        }

        MeasurementKey alarmKey = MeasurementKey.LookUpBySignalID(signalID);

        if (alarmKey == MeasurementKey.Undefined)
        {
            OnStatusMessage(MessageLevel.Error, $"[Python Proxy Subscriber]: Failed to process Python event buffer-block, cannot find measurement key for signal {signalID:D}");
            return null;
        }

        Ticks startTime = startTimeTicks;
        Ticks endTime = endTimeTicks;

        // AlarmMeasurement.Timestamp is the wall-clock at receipt; AlarmTimestamp is the relevant
        // event moment (start time on event start, end time on event end). Matches the pattern in
        // `waveAppsDataTransfer/DataSubscriber.cs::ProcessEventDetailsQueue` so the host sees the
        // same shape regardless of which path the event came in on.
        AlarmMeasurement alarmMeasurement = new()
        {
            Timestamp = DateTime.UtcNow,
            AlarmTimestamp = value switch
            {
                0.0D when endTime > 0L => endTime,
                > 0.0D or < 0.0D when startTime > 0L => startTime,
                _ => DateTime.UtcNow
            },
            Value = value,
            AlarmID = eventID,
            Metadata = alarmKey.Metadata
        };

        using AdoDataConnection connection = new(ConfigSettings.Instance);
        TableOperations<EventDetails> tableOperations = new(connection);

        if (value > 0.0D)
        {
            // Start of event - write the record using whatever start/end the publisher had.
            // EndTime is typically 0 here (the publisher does not yet know when the event ends).
            EventDetails record = new()
            {
                StartTime = startTime,
                EndTime = endTime > 0L ? endTime : DateTime.MinValue,
                EventGuid = eventID,
                Type = eventType,
                MeasurementID = signalID,
                Details = eventDetails
            };

            tableOperations.AddNewRecord(record);
        }
        else
        {
            // End of event - find the existing record and update its EndTime.
            EventDetails? record = tableOperations.QueryRecordWhere("EventGuid = {0}", eventID);

            if (record is null)
            {
                OnStatusMessage(MessageLevel.Error, $"[Python Proxy Subscriber]: Failed to find existing event record \"{eventID}\" to update end of event");
                return alarmMeasurement; // Still publish the alarm; just couldn't update the EventDetails row
            }

            record.EndTime = endTime;
            tableOperations.UpdateRecord(record);
        }

        return alarmMeasurement;
    }

    private void m_proxyDataSubscriber_MetaDataReceived(object? sender, EventArgs<DataSet> e)
    {
        OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Received metadata from Python calculation adapter");
    }

    private void m_proxyDataSubscriber_MetadataSyncComplete(object? sender, EventArgs e)
    {
        OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Metadata synchronization from Python calculation adapter complete");
        OnConfigurationChanged();
        m_awaitingHostMetadataSync = true;
    }

    private void m_proxyDataSubscriber_ReceivedUserCommandResponse(object? sender, DataSubscriber.UserCommandArgs e)
    {
        ServerResponse response = e.Response;
        ServerCommand command = e.Command;
        int length = e.Length;

        switch (response)
        {
            case ServerResponse.UserResponse00 when command == ServerCommand.UserCommand00:
                OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Received configuration changed notification from Python calculation adapter");
                m_proxyDataSubscriber?.RefreshMetadata();
                break;
            // Note: event publications from the Python calculation adapter used to be transported here
            // as `UserResponse03 / UserCommand03` carrying a connection-string-encoded record. They now
            // flow as STTP BufferBlock measurements through `m_proxyDataSubscriber_NewMeasurements ->
            // ProcessEventBufferBlock`. Reach for a fresh user-command pair if a future feature needs
            // a side-channel notification.
            default:
                OnStatusMessage(MessageLevel.Warning, $"[Python Proxy Subscriber]: Received unhandled {length:N0}-byte user server response {response} for command {command} from Python calculation adapter");
                break;
        }
    }

    // --- Python Process Event Handlers ---

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