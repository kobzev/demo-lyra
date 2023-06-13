namespace Lyra.Instruments.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;
    using System.Text;
    using Lyra.Instruments.Configuration.Validation;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;

    public class LyraConfiguration
    {
        /// <summary>
        /// Settings to authenticate against Iconia. 
        /// </summary>
        public AuthSettings Auth { get; set; } = new AuthSettings();

        /// <summary>
        /// Cloudfront Url
        /// Defined: deploy-infra
        /// Retrieved: env var
        /// </summary>
        [Required, BaseUrl] // ../../foo/
        public string CloudFrontUrl { get; set; }

        public void LogConfig(IConfiguration configRoot, ILogger log)
        {
            var printedConfig = this.PrintConfig(configRoot);

            // Add the settings (and optionally the errors) as metadata to the logmessage
            // not part of the actual logmessage itself. Otherwise, the logmessage
            // explodes in logdna. Now it's a one liner, that, when you open it up, it shows the information
            var logScope = new Dictionary<string, object>()
            {
                { "configsettings", printedConfig },
            };

            if (!this.IsValid(out var errors))
            {
                var errorsString = string.Join(Environment.NewLine, errors.Select(x => x.ToString()));
                logScope.Add("errors", errorsString);
                using (log.BeginScope(logScope))
                {
                    log.LogError("Configuration errors detected");
                }
            }
            else
            {
                using (log.BeginScope(logScope))
                {
                    log.LogDebug("Configuration loaded");
                }
            }
        }

        public bool IsValid(out IReadOnlyCollection<ConfigurationError> errors)
        {
            return ConfigurationValidator.TryValidate(this, out errors);
        }

        public string PrintConfig(IConfiguration config)
        {
            var subject = this;
            var builder = new StringBuilder();

            // First, print out normal properties
            ConfigurationValidator.PrintProperties(config, subject, builder);

            return builder.ToString();
        }
    }
}
