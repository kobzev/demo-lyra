using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Lyra.Instruments
{
    using Amazon.DynamoDBv2;
    using Amazon.XRay.Recorder.Core;
    using Amazon.XRay.Recorder.Handlers.AwsSdk;
    using Amazon.Lambda.APIGatewayEvents;
    using Amazon.Lambda.Core;
    using LykkeCorp.Bill.Logging;
    using Lyra.Instruments.Configuration;
    using Lyra.Instruments.Infrastructure;
    using Lyra.Instruments.Models.Instruments;
    using Lyra.Persistence;
    using Lyra.Repository;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Serilog;
    using System.Collections.Generic;
    using System.Net;

    public class LambdaEntryPoint
    {
        private readonly LyraConfiguration config;

        private ServiceProvider provider;

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public LambdaEntryPoint() : this
        (new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddEnvironmentVariables().Build(), new ServiceCollection())
        {
        }

        public LambdaEntryPoint(IConfiguration configuration, IServiceCollection serviceCollection)
        {
            var buildInfo = BuildInfoReader.Read();

            AWSXRayRecorder.InitializeInstance(configuration);
            AWSSDKHandler.RegisterXRayForAllServices();

            var serilogLogger = LogConfiguration.BuildSerilogLogger(configuration, "lyra-get-instruments",
                ("project", "lyra"),
                ("app-type", "lambda"),
                ("build-git-hash", buildInfo.commitHash),
                ("build-time", buildInfo.time.ToString("f")));

            this.config = new LyraConfiguration();
            configuration.Bind(this.config);

            serviceCollection.AddLogging(builder => builder.AddSerilog(serilogLogger));

            serviceCollection.AddSingleton(this.config);

            // Optional services, to be overwritten in unit tests. 
            serviceCollection.TryAddSingleton<IAmazonDynamoDB, AmazonDynamoDBClient>();
            serviceCollection.TryAddSingleton<IProductWriteRepository, ProductWriteRepository>();
            serviceCollection.TryAddSingleton<IProductReadRepository, ProductReadRepository>();

            this.provider = serviceCollection.BuildServiceProvider();
        }

        /// <summary>
        /// A Lambda function to respond to HTTP Get methods from API Gateway
        /// </summary>
        /// <param name="request"></param>
        /// <returns>The API Gateway response.</returns>
        public APIGatewayProxyResponse GetInstrument(APIGatewayProxyRequest request, ILambdaContext context)
        {
            var logger = this.provider.GetRequiredService<ILogger<LambdaEntryPoint>>();
            logger.LogInformation("GetInstrument Request\n");

            var repo = this.provider.GetRequiredService<IProductReadRepository>();

            var instrumentRequest = JsonConvert.DeserializeObject<InstrumentRequest>(request.Body);

            var instrument = repo.GetInstrumentAsync(instrumentRequest.TenantId, instrumentRequest.InstrumentId).Result;

            if (instrument == null)
            {
                return new APIGatewayProxyResponse
                {
                    StatusCode = (int)HttpStatusCode.NotFound,
                    Body = string.Empty,
                    Headers = new Dictionary<string, string>
                    {
                        { "Content-Type", "application/json" },
                        { "Access-Control-Allow-Origin", "*" }
                    }
                };
            }

            var model = new InstrumentModel
            {
                Id = instrument,
                Name = instrument.Name,
                NumberOfDecimalPlaces = instrument.NumberOfDecimalPlaces,
                InstrumentStatus = instrument.InstrumentStatus
            };

            string body = (model != null) ? JsonConvert.SerializeObject(model) : string.Empty;

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = body,
                Headers = new Dictionary<string, string> 
                {   
                    { "Content-Type", "application/json" },
                    { "Access-Control-Allow-Origin", "*" } 
                }
            };

            return response;
        }
    }
}
