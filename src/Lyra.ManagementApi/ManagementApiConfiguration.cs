namespace Lyra.ManagementApi
{
    using Lyra.Api.Configuration;

    public class ManagementApiConfiguration
    {
        public AuthSettings Auth { get; set; }

        public string CloudFrontUrl { get; set; }
    }
}
