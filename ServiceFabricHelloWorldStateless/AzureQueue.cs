using Azure.Storage.Queues;
using System;

namespace ServiceFabricHelloWorldStateless
{
    //https://docs.microsoft.com/en-us/azure/storage/queues/storage-dotnet-how-to-use-queues
    public class AzureQueue
    {
        public string ErrorMessage { get => errorMessage; }

        private string errorMessage;

        private string accountName { get; set; }
        private string accountKey { get; set; }

        public AzureQueue(string accountName, string accountKey)
        {
            this.accountName = accountName;
            this.accountKey = accountKey;
        }

        public bool SetQueue(string queueName, string payload)
        {
            errorMessage = "";
            try
            {
                string connectStr = "DefaultEndpointsProtocol=https;" +
                 "AccountName=" + accountName + ";" +
                 "AccountKey=" + accountKey + ";EndpointSuffix=core.windows.net";
                QueueClientOptions qOpt = new QueueClientOptions
                {
                    MessageEncoding = QueueMessageEncoding.Base64
                };
                QueueClient queueClient = new QueueClient(connectStr, queueName, qOpt);
                queueClient.Create();
                queueClient.SendMessage(payload);

                return true;
            }
            catch (Exception ex)
            {
                errorMessage = "QueueName: " + queueName + " - " + ex.ToString();
            }
            return false;
        }
    }
}
