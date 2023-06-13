namespace Lyra.HostedService
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.Runtime;
    using LykkeCorp.Hosting;
    using Lyra.Persistence;
    using Microsoft.Extensions.Logging;

    public class TenantManagement : HostedServiceBase
    {
        private readonly Lazy<LyraHostedServiceSettings> lazySettings;
        private readonly ILoggerFactory loggerFactory;

        public TenantManagement(
            Lazy<LyraHostedServiceSettings> lazySettings,
            ILoggerFactory loggerFactory)
            : base(loggerFactory.CreateLogger<HostedServiceBase>())
        {
            this.lazySettings = lazySettings;
            this.loggerFactory = loggerFactory;
        }

        protected override async Task OnStart(CancellationToken cancellationToken)
        {
            var settings = this.lazySettings.Value;

            var credentials = new BasicAWSCredentials("not", "used");
            var dynamoDBClient = new AmazonDynamoDBClient(
                credentials,
                new AmazonDynamoDBConfig
                {
                    ServiceURL = settings.DynamoDBServiceUrl.ToString()
                });

            var dynamoDbProvisioner = new DynamoDbTableProvisioner(dynamoDBClient, this.loggerFactory.CreateLogger<DynamoDbTableProvisioner>());

            await dynamoDbProvisioner.ProvisionLykke(settings.TenantIds.ToList(), Table.Products, cancellationToken);
        }

        protected override Task OnStop(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
