using Microsoft.Extensions.Configuration;

namespace PersistMessagesInDataLake
{
    public class DataLakeCredentials
    {
        public string ClientId { get; set; }

        // Portal > Azure AD > App Registrations > App > Settings > Keys (aka Client Secret)
        public string ClientSecret { get; set; }

        // Portal > Azure AD > Properties > Directory ID (aka Tenant ID)
        public string TenantId { get; set; }

        // Name of the Azure Data Lake Store
        public string AdlsAccountName { get; set; }

        public DataLakeCredentials(IConfigurationRoot config)
        {
            ClientId = config["Adls:ClientId"];
            ClientSecret = config["Adls:ClientSecret"];
            TenantId = config["Adls:TenantId"];
            AdlsAccountName = config["Adls:AdlsAccountName"];
        }
    }
}