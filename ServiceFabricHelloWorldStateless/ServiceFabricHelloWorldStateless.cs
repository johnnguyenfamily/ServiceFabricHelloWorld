using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.AppConfiguration;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace ServiceFabricHelloWorldStateless
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class ServiceFabricHelloWorldStateless : StatelessService
    {
        public ServiceFabricHelloWorldStateless(StatelessServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            // Connect to an App Configuration Client using a connectionstring
            string _connectionString = "<App-Configuration-Endpoint>";
            ConfigurationClient _client = new ConfigurationClient(_connectionString);

            // Read all settings using a filter
            var settingsSelector = new SettingSelector() { KeyFilter = "*" };
            var settings = _client.GetConfigurationSettings(settingsSelector);
            var labels = settings.GroupBy(s => s.Label);

            List<dynamic> list = new List<dynamic>();
            foreach (var label in labels)
            {
                dynamic s = new ExpandoObject();
                s.label = label.Key != null ? label.Key : string.Empty;
                var setingsByLabel = settings.Where(s => s.Label == label.Key);
                var values = new Dictionary<string, object>();
                foreach (var setting in setingsByLabel)
                {
                    values.Add(setting.Key, setting.Value);
                }
                s.values = values;
                list.Add(s);
            }

            ServiceEventSource.Current.ServiceMessage(this.Context, "LabelNum-{0}", list.Count);
            ServiceEventSource.Current.ServiceMessage(this.Context, "Label1-{0}", list[0].label);
            ServiceEventSource.Current.ServiceMessage(this.Context, "Label2-{0}", list[1].label);
            ServiceEventSource.Current.ServiceMessage(this.Context, "Label1Values-{0}", list[0].values.Count);
            ServiceEventSource.Current.ServiceMessage(this.Context, "Label2Values-{0}", list[1].values.Count);

            ServiceEventSource.Current.ServiceMessage(this.Context, "AppConfigKey1-{0}", list[1].values["AppConfigKey1"]);
            ServiceEventSource.Current.ServiceMessage(this.Context, "AppConfigKey2-{0}", list[1].values["AppConfigKey2"]);
            ServiceEventSource.Current.ServiceMessage(this.Context, "AppConfigKey3-{0}", list[1].values["AppConfigKey3"]);

            // Generate a JSON message
            string payload =
                "{\r\n" +
                    "\"AppConfigKey1\": " + "\"" + list[1].values["AppConfigKey1"] + "\", \r\n" +
                    "\"AppConfigKey2\": " + "\"" + list[1].values["AppConfigKey2"] + "\", \r\n" +
                    "\"AppConfigKey3\": " + "\"" + list[1].values["AppConfigKey3"] + "\"\r\n" +
                "}";

            // Add JSON message to the queue
            bool success = false;
            string accountName = "<Storage-Account-Name>";
            string accountKey = "<Storage-Account-Key>";
            string qName = "<Queue-Name>";
            AzureQueue aq = new AzureQueue(accountName, accountKey);
            success = aq.SetQueue(qName, payload);
            if (!success)
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "MessageQueueFails-{0}", qName);
            }


            long iterations = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                ServiceEventSource.Current.ServiceMessage(this.Context, "Working-{0}", ++iterations);

                await Task.Delay(TimeSpan.FromSeconds(60), cancellationToken);
            }
        }
    }
}
