namespace Lyra.HostedService
{
    using System;

    public class LyraHostedServiceSettings
    {
        public LyraHostedServiceSettings()
        {
        }

        public LyraHostedServiceSettings(string lyraInternalAddress,
            string lyraManagementApiAddress,
            Uri internalStsUrl,
            string cloudFrontUrl,
            string clientId,
            string clientSecret,
            Uri dynamoDbServiceUrl,
            string[] tenantIds,
            GetSerilogLoggerDelegate serilogLogger)
        {
            this.LyraInternalAddress = lyraInternalAddress;
            this.CloudFrontUrl = cloudFrontUrl;
            this.InternalStsUrl = internalStsUrl;
            this.ClientId = clientId;
            this.ClientSecret = clientSecret;
            this.DynamoDBServiceUrl = dynamoDbServiceUrl;
            this.TenantIds = tenantIds;
            this.GetSerilogLogger = serilogLogger;
            this.LyraManagementApiAddress = lyraManagementApiAddress;
        }

        public string LyraManagementApiAddress { get; }
        
        public string LyraInternalAddress { get; set; }

        public string CloudFrontUrl { get; set; }

        public Uri InternalStsUrl { get; set; }

        public string ClientId { get; set; }

        public string ClientSecret { get; set; }

        public Uri DynamoDBServiceUrl { get; set; }

        public string[] TenantIds { get; set; }

        public GetSerilogLoggerDelegate GetSerilogLogger { get; set; }
    }
}
