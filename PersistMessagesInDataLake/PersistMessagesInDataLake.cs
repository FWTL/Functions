using FWTL.Events.Telegram.Messages;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using Newtonsoft.Json;
using System.IO;
using System.Text;
using System.Threading;

namespace PersistMessagesInDataLake
{
    public static class PersistMessagesInDataLake
    {
        [FunctionName("PersistMessagesInDataLake")]
        public static void Run([EventHubTrigger("fwtleventhub", Connection = "EventHubConnectionAppSetting")] EventData[] eventHubMessages, ILogger log)
        {
            //DataLakeCredentials credentials = Config(null);
            AdlsClient client = Auth(null);

            foreach (var message in eventHubMessages)
            {
                Data data = JsonConvert.DeserializeObject<Data>(Encoding.UTF8.GetString(message.Body));
                string json = JsonConvert.SerializeObject(data.Message);

                using (var stream = client.CreateFile(CreatePath(data), IfExists.Overwrite))
                {
                    byte[] array = Encoding.UTF8.GetBytes(json);
                    stream.Write(array, 0, array.Length);
                }
            }
        }

        //private static DataLakeCredentials Config(ExecutionContext context)
        //{
        //    var config = new ConfigurationBuilder()
        //    .SetBasePath(context.FunctionAppDirectory)
        //    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
        //    .AddEnvironmentVariables()
        //    .Build();

        //    return new DataLakeCredentials(config);
        //}

        private static AdlsClient Auth(DataLakeCredentials credentials)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var clientCredential = new ClientCredential(credentials.ClientId, credentials.ClientSecret);
            var creds = ApplicationTokenProvider.LoginSilentAsync(credentials.TenantId, clientCredential).Result;
            return AdlsClient.CreateClient(credentials.AdlsAccountName, creds);
        }

        public static string CreatePath(Data data)
        {
            var pathToFolder = CreateMessagePath(CreatePeerPath(data), data.Message);
            return Path.Combine(pathToFolder, data.Message.Id + ".json");
        }

        private static string CreateMessagePath(string rootPath, Message message)
        {
            string year = message.CreateDate.Year.ToString();
            string month = message.CreateDate.Month.ToString();
            string day = message.CreateDate.Day.ToString();

            return Path.Combine(rootPath, year, month, day);
        }

        public static string CreatePeerPath(Data data)
        {
            string peerType = ((int)data.PeerType).ToString();
            string sourceId = data.SourceId.ToString();

            return Path.Combine(peerType, sourceId);
        }
    }
}