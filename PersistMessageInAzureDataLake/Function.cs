using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using PersistMessagesInDataLake;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace PersistMessageInAzureDataLake
{
    public static class Function
    {
        [FunctionName("PersistMessageInAzureDataLake")]
        public static void Run([EventHubTrigger("fwtleventhub", Connection = "EventHubConnectionAppSetting")] EventData[] eventHubMessages, ILogger log, ExecutionContext context)
        {
            IConfigurationRoot config = new ConfigurationBuilder()
            .SetBasePath(context.FunctionAppDirectory)
            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

            var auth = new Auth(config);

            AdlsClient client = auth.AzureDataLake();
            var job = new PersistMessagesInDataLakeJob(client);
            job.Run(eventHubMessages);
        }
    }
}