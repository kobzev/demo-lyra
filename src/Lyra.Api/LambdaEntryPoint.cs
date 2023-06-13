namespace Lyra.Api
{
    using Amazon.Lambda.AspNetCoreServer;
    using Amazon.XRay.Recorder.Core;
    using Amazon.XRay.Recorder.Handlers.AwsSdk;
    using LykkeCorp.Bill.Logging;
    using Lyra.Api.Infrastructure;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.Configuration;
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

            var buildInfo = BuildInfoReader.Read();

            var serilogLogger = LogConfiguration.BuildSerilogLogger(configuration, "lyra-api",
                 ("project", "lyra"),
                 ("app-type", "web-lambda"),
                 ("build-git-hash", buildInfo.commitHash),
                 ("build-time", buildInfo.time.ToString("f")));

            AWSXRayRecorder.InitializeInstance(configuration);
            AWSSDKHandler.RegisterXRayForAllServices();

            builder
                .ConfigureWebHost(webHost => webHost
                .UseStartup(e=> new Startup(configuration, () => null)))
                .UseSerilog(serilogLogger);
        }
    }
}
