//******************************************************************************************************
//  MyCalcDataProxyAdapter.cs - Gbtc
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
/// Represents the adapter that exists in the WaveApps host application used to proxy data to and from
/// the Python calculation.
/// </summary>
// TODO: Rename class (and file name) t0 better represent user calculation intent
public class MyCalcDataProxyAdapter : PythonDataProxyBase
{
    // Define properties used to configure Python adapter. These configuration values
    // will be sent to the Python calculation adapter during initialization. Below are
    // some standard properties used to configure time-alignment and data processing
    // parameters. Users should add their own properties specific to the calculation
    // algorithm configuration using a similar pattern.

    /// <summary>
    /// Gets or sets the measurement window size, in whole seconds, for data grouping
    /// </summary>
    [Description("Defines measurement window size, in whole seconds, for data grouping")]
    [ConnectionStringParameter]
    [DefaultValue(5)]
    [AmbientValue("measurement_windowsize")] // Corresponding Python parameter name
    public int MeasurementWindowSize { get; set; }

    /// <summary>
    /// Gets or sets the number of samples per second for the data in the stream.
    /// </summary>
    [ConnectionStringParameter]
    [DefaultValue(3000)]
    [Description("Defines the number of samples per second for the data in the stream")]
    [AmbientValue("samplespersecond")]
    public int SamplesPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the lag time, in seconds, for data grouping (can be sub-second).
    /// </summary>
    /// <remarks>
    /// <para>Defines the time sensitivity to past measurement timestamps.</para>
    /// <para>The number of seconds allowed before assuming a measurement timestamp is too old.</para>
    /// <para>Data received outside this past time limit, relative to local clock, will be discarded.</para>
    /// </remarks>
    [ConnectionStringParameter]
    [DefaultValue(2.0D)]
    [Description("Defines the lag time, in seconds, for data grouping (can be sub-second)")]
    [AmbientValue("lagtime")]
    public new double LagTime { get; set; }

    /// <summary>
    /// Gets or sets the lead time, in seconds, for data grouping (can be sub-second)
    /// </summary>
    /// <remarks>
    /// <para>Defines the time sensitivity to future measurement timestamps.</para>
    /// <para>The number of seconds allowed before assuming a measurement timestamp is too advanced.</para>
    /// <para>Data received outside this future time limit, relative to local clock, will be discarded.</para>
    /// </remarks>
    [ConnectionStringParameter]
    [DefaultValue(2.0D)]
    [Description("Defines the lead time, in seconds, for data grouping (can be sub-second)")]
    [AmbientValue("leadtime")]
    public new double LeadTime { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if the data proxy should display a summary of received measurements every few seconds.
    /// </summary>
    [ConnectionStringParameter]
    [DefaultValue(false)]
    [Description("Defines flag that determines if the data proxy should display a summary of received measurements every few seconds")]
    [AmbientValue("display_measurementsummary")]
    public bool DisplayMeasurementSummary { get; set; }

    // -------------------------------------------------------------
    //
    // TODO: Add custom calculation configuration parameters here...
    //
    // -------------------------------------------------------------
}