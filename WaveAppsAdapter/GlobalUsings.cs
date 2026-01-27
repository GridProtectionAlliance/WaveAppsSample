// Global using directives

global using System.ComponentModel;
global using System.Data;
global using System.Text;
global using Gemstone;
global using Gemstone.Data;
global using Gemstone.Data.Model;
global using Gemstone.Diagnostics;
global using Gemstone.StringExtensions;
global using Gemstone.Timeseries;
global using Gemstone.Timeseries.Adapters;
global using Gemstone.Timeseries.Model;
global using sttp;
global using ConnectionStringParser = Gemstone.Configuration.ConnectionStringParser<Gemstone.Timeseries.Adapters.ConnectionStringParameterAttribute>;
global using AmbientConnectionStringComposer = Gemstone.Configuration.ConnectionStringParser<System.ComponentModel.AmbientValueAttribute>;
global using ConfigSettings = Gemstone.Configuration.Settings;