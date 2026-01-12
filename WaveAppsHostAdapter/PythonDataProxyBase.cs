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
//  01/09/2026 - Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using System.ComponentModel;
using Gemstone.Timeseries.Adapters;

namespace WaveAppsHostAdapter;

/// <summary>
/// Represents a base class for proxying data to and from a Python calculation adapter.
/// </summary>
/// <remarks>
/// This is internal functionality used to send and receive data from the Python adapter.
/// It is not expected that user will need to modify this code.
/// </remarks>
public class PythonDataProxyBase : FacileActionAdapterBase
{
    // Ensure sttp.gemstone available from NuGet
    //private DataPublisher m_proxyDataPublisher;
    //private DataSubscriber m_proxyDataSubscriber;

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

    /// <inheritdoc />
    public override string GetShortStatus(int maxLength)
    {
        return $"Processed {ProcessedMeasurements:N0} measurements so far...";
    }
}