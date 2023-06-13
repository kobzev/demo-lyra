namespace Lyra.HostedService
{
    using System;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.Runtime;
    using LykkeCorp.Hosting;
    using Lyra.Api;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using Serilog;

    internal class InternalApiHostedService : HostedServiceBase
    {
        private readonly Lazy<LyraHostedServiceSettings> lazySettings;
        private IHost host;

        public InternalApiHostedService(Lazy<LyraHostedServiceSettings> lazySettings,
            ILogger<HostedServiceBase> logger)
            : base(logger)
        {
            this.lazySettings = lazySettings;
        }

        protected override async Task OnStart(CancellationToken cancellationToken)
        {
            var settings = lazySettings.Value;
            var lyraConfiguration = new ConfigurationBuilder()
                .AddInMemoryCollection(SettingsBuilder.BuildDictionary(settings))
                .Build();

            var awsCredentials = new BasicAWSCredentials("ignored", "ignored");
            var dynamoDbConfig = new AmazonDynamoDBConfig
            {
                ServiceURL = settings.DynamoDBServiceUrl.ToString()
            };

            this.host = new HostBuilder().ConfigureWebHost(
                webHost =>
                {
                    webHost.UseKestrel()
                        .UseEnvironment(Environments.Development)
                        .ConfigureServices(s =>
                        {
                            s.AddSingleton<IAmazonDynamoDB>(new AmazonDynamoDBClient(awsCredentials, dynamoDbConfig));
                        })
                        .UseStartup(x => new Startup(lyraConfiguration, () => null))
                        .UseUrls(settings.LyraInternalAddress)
                        .SuppressStatusMessages(true);
                })                        
                .UseSerilog(settings.GetSerilogLogger("LyraInternal"))
                .Build();

            await this.host.StartAsync(cancellationToken);
        }

        protected override async Task OnStop(CancellationToken cancellationToken)
        {
            await this.host.StopAsync(cancellationToken);
        }
    }
}
