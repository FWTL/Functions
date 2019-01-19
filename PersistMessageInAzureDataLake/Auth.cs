using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using PersistMessagesInDataLake;

namespace PersistMessageInAzureDataLake
{
    public class Auth
    {
        private readonly IConfigurationRoot _config;

        public Auth(IConfigurationRoot config)
        {
            _config = config;
        }

        public AdlsClient AzureDataLake()
        {
            var credentials = new DataLakeCredentials(_config);
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var clientCredential = new ClientCredential(credentials.ClientId, credentials.ClientSecret);
            var creds = ApplicationTokenProvider.LoginSilentAsync(credentials.TenantId, clientCredential).Result;
            return AdlsClient.CreateClient(credentials.AdlsAccountName, creds);
        }
    }
}
