namespace Lyra.ManagementApi
{
    using System;
    using System.Net.Http;
    using Amazon.Lambda.AspNetCoreServer;
    using Amazon.XRay.Recorder.Core;
    using Amazon.XRay.Recorder.Handlers.AwsSdk;
    using LykkeCorp.Bill.Logging;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Serilog;

    public class LambdaEntryPoint : ApplicationLoadBalancerFunction
    {
        protected override void Init(IHostBuilder builder)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", false)
                .AddEnvironmentVariables()
                //.AddLyraSecrets()
                .Build();

            var serilogLogger = LogConfiguration.BuildSerilogLogger(configuration, "lyra-api", ("project", "lyra"), ("app-type", "web-lambda"));

            AWSXRayRecorder.InitializeInstance(configuration);
            AWSSDKHandler.RegisterXRayForAllServices();

            builder
                .ConfigureWebHost(webHost => webHost
                .UseStartup(e=> new ManagementApiStartup(configuration, () => null)))
                .UseSerilog(serilogLogger);
        }
    }
}
