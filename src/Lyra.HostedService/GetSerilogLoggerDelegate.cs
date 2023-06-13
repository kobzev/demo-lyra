namespace Lyra.HostedService
{
    using Serilog;

    /// <summary>
    /// Supports getting a configured serilog logger to be used in this hosted service.
    /// </summary>
    /// <param name="applicationName"></param>
    /// <returns></returns>
    public delegate ILogger GetSerilogLoggerDelegate(string applicationName);
}
