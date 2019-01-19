using System.IO;
using System.Text;
using FWTL.Events.Telegram.Messages;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;

namespace PersistMessagesInDataLake
{
    public class PersistMessagesInDataLakeJob
    {
        private readonly AdlsClient _client;

        public PersistMessagesInDataLakeJob(AdlsClient client)
        {
            _client = client;
        }

        public void Run(EventData[] eventHubMessages)
        {
            foreach (var message in eventHubMessages)
            {
                Data data = JsonConvert.DeserializeObject<Data>(Encoding.UTF8.GetString(message.Body));
                string json = JsonConvert.SerializeObject(data.Message);

                using (var stream = _client.CreateFile(CreatePath(data), IfExists.Overwrite))
                {
                    byte[] array = Encoding.UTF8.GetBytes(json);
                    stream.Write(array, 0, array.Length);
                }
            }
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