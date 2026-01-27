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

namespace WaveApps;

/// <summary>
/// Represents a base class for proxying data to and from a Python calculation adapter.
/// </summary>
/// <remarks>
/// This is internal functionality used to send and receive data from the Python adapter.
/// It is not expected that user will need to modify this code.
/// </remarks>
public class PythonDataProxyBase : FacileActionAdapterBase
{
    #region [ Members ]
    
    // Nested Types
    private class ProxyDataPublisher : DataPublisher
    {
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
            return SendClientResponse(clientID, response, command, data);
        }
    }
    
    private class ProxyDataSubscriber : DataSubscriber
    {
        /// <inheritdoc />
        public override DataSet? DataSource
        {
            get => base.DataSource;
            set
            {
                if (DataSetEqualityComparer.Default.Equals(DataSource, value))
                    return;

                base.DataSource = value;

                if (CommandChannelConnected)
                    SendServerCommand(ServerCommand.UserCommand00);
            }
        }
    }

    // Fields
    private ProxyDataPublisher? m_proxyDataPublisher;
    private ProxyDataSubscriber? m_proxyDataSubscriber;
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
                m_proxyDataSubscriber.DataSource = value;

            if (m_proxyDataPublisher is not null)
                m_proxyDataPublisher.DataSource = value;
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

    /// <inheritdoc />
    public override bool SupportsTemporalProcessing => false;

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

        m_proxyDataPublisher = new ProxyDataPublisher(); // Initialize with HostAdapterPublisherPort

        // Attach to publisher events
        m_proxyDataPublisher.StatusMessage += m_proxyDataPublisher_StatusMessage;
        m_proxyDataPublisher.ProcessException += m_proxyDataPublisher_ProcessException;
        m_proxyDataPublisher.ClientConnected += m_proxyDataPublisher_ClientConnected;

        m_proxyDataPublisher.Name = $"{Name}_DataProxyPublisher";
        m_proxyDataPublisher.UseBaseTimeOffsets = true;
        m_proxyDataPublisher.AllowPayloadCompression = true;
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
        m_proxyDataSubscriber.ReceivedUserCommandResponse += m_proxyDataSubscriber_ReceivedUserCommandResponse;

        m_proxyDataSubscriber.ConnectionString = $"server=localhost:{PythonCalcPublisherPort}";
        m_proxyDataSubscriber.OperationalModes |= OperationalModes.CompressMetadata | OperationalModes.CompressSignalIndexCache | OperationalModes.CompressPayloadData | OperationalModes.ReceiveInternalMetadata | OperationalModes.ReceiveExternalMetadata;
        m_proxyDataSubscriber.CompressionModes = CompressionModes.TSSC | CompressionModes.GZip;
        m_proxyDataSubscriber.Initialize();

        // Start subscriber
        m_proxyDataSubscriber.Start();
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

        AmbientConnectionStringComposer composer = new();

        // Generate a serialized connection string based on current ambient property values
        string connectionString = composer.ComposeConnectionString(this);

        // Send serialized property values to Python adapter, using user response 2
        bool success = m_proxyDataPublisher?.SendUserCommandResponse(clientID, ServerResponse.UserResponse02, ServerCommand.UserCommand02, Encoding.UTF8.GetBytes(connectionString)) ?? false;
        
        if (success)
            OnStatusMessage(MessageLevel.Info, $"Successfully sent serialization of adapter properties to Python calculation adapter \"{connectionID}\".");
        else
            OnStatusMessage(MessageLevel.Error, $"Failed to send serialization of adapter properties to Python calculation adapter \"{connectionID}\".");
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
        OnStatusMessage(MessageLevel.Info, "[Python Proxy Subscriber]: Connection established, subscribing to outputs...");

        // We subscribe to all outputs from Python calculation adapter
        SubscriptionInfo subscriptionInfo = new() { FilterExpression = "FILTER ActiveMeasurements WHERE True" };
        bool success = m_proxyDataSubscriber?.Subscribe(subscriptionInfo) ?? false;
        
        if (success)
            OnStatusMessage(MessageLevel.Info, "Successfully subscribed to Python calculation adapter outputs.");
        else
            OnStatusMessage(MessageLevel.Error, "Failed to subscribe to Python calculation adapter outputs.");
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
        OnStatusMessage(MessageLevel.Info, "Received metadata from Python calculation adapter");

        // TODO: Automatically create new local measurements based on received metadata, then ensure host OutputMeasurements is updated accordingly
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
                OnStatusMessage(MessageLevel.Info, "Received configuration changed notification from Python calculation adapter");
                m_proxyDataSubscriber?.RefreshMetadata();
                break;
            // The 'UserResponse02' used for sending serialized property values to Python adapter is on a different
            // communication channel (proxy publisher), so we could have used 'UserResponse02' here for processing
            // event publication requests from Python calculation adapter. However, we instead chose the distinct
            // 'UserResponse03' for clarity and to reduce confusion when reviewing the code.
            case ServerResponse.UserResponse03 when command == ServerCommand.UserCommand03:
            {
                OnStatusMessage(MessageLevel.Info, $"Processing {length:N0}-byte event publication request from Python calculation adapter");

                string connectionString = Encoding.UTF8.GetString(buffer, startIndex, length);
                Dictionary<string, string> settings = connectionString.ParseKeyValuePairs();

                if (!settings.TryGetValue("SignalID", out string? setting) || !Guid.TryParse(setting, out Guid signalID))
                {
                    OnStatusMessage(MessageLevel.Error, "Cannot process Python event publication request, failed to parse 'SignalID' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("EventID", out setting) || !Guid.TryParse(setting, out Guid eventID))
                {
                    OnStatusMessage(MessageLevel.Error, "Cannot process Python event publication request, failed to parse 'EventID' parameter");
                    return;
                }

                if (!settings.TryGetValue("Timestamp", out setting) || !Ticks.TryParse(setting, out Ticks timestamp))
                {
                    OnStatusMessage(MessageLevel.Error, "Cannot process Python event publication request, failed to parse 'Timestamp' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("AlarmTimestamp", out setting) || !Ticks.TryParse(setting, out Ticks alarmTimestamp))
                {
                    OnStatusMessage(MessageLevel.Error, "Cannot process Python event publication request, failed to parse 'AlarmTimestamp' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("Value", out setting) || !double.TryParse(setting, out double value))
                {
                    OnStatusMessage(MessageLevel.Error, "Cannot process Python event publication request, failed to parse 'Value' parameter");
                    return;
                }
                
                if (!settings.TryGetValue("EventDetails", out string? eventDetailsJson))
                {
                    OnStatusMessage(MessageLevel.Error, "Cannot process Python event publication request, failed to parse 'EventDetails' parameter");
                    return;
                }

                MeasurementKey alarmKey = MeasurementKey.LookUpBySignalID(signalID);

                if (alarmKey == MeasurementKey.Undefined)
                {
                    OnStatusMessage(MessageLevel.Error, "Failed to process Python event publication request, cannot find measurement key for specified 'SignalID'");
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
                        Details = eventDetailsJson
                    };

                    tableOperations.AddNewRecord(record);
                }
                else
                {
                    // End of event
                    EventDetails? record = tableOperations.QueryRecordWhere("EventGuid = {0}", eventID);

                    if (record is null)
                    {
                        OnStatusMessage(MessageLevel.Error, $"Failed to find existing event record \"{eventID}\" to update end of event");
                        return;
                    }

                    record.EndTime = alarmTimestamp;
                    tableOperations.UpdateRecord(record);
                }

                OnNewMeasurements([alarmMeasurement]);
                break;
            }
            default:
                OnStatusMessage(MessageLevel.Warning, $"Received unhandled {length:N0}-byte user server response {response} for command {command} from Python calculation adapter");
                break;
        }
    }

    #endregion
}