namespace Lyra.HostedService
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using LykkeCorp.Hosting;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;

    public class LyraHostedService : HostedServiceBase
    {
        private readonly IHostedService inner;
        public LyraHostedService(
          Lazy<LyraHostedServiceSettings> lazySettings,
          ILoggerFactory loggerFactory,
          ILogger<HostedServiceBase> logger)
          : base(logger)
        {
            this.inner = new SequentialHostedService(
                "lyra",
                logger,
                new ParallelHostedService(
                    "lyra-apps",
                    logger,
                    new InternalApiHostedService(lazySettings, logger),
                    new TenantManagement(lazySettings, loggerFactory),
                    new ManagementApiHostedService(lazySettings, logger)
                    )
                )
            {
                Parent = this
            };
        }

        protected override Task OnStart(CancellationToken cancellationToken)
            => this.inner.StartAsync(cancellationToken);

        protected override Task OnStop(CancellationToken cancellationToken)
            => this.inner.StopAsync(cancellationToken);
    }
}
