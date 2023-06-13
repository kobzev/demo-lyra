namespace Lyra.Instruments.Configuration
{
    using System.ComponentModel.DataAnnotations;
    using Lyra.Instruments.Configuration.Validation;

    public class AuthSettings
    {
        [Required] public string ClientId { get; set; }

        [Required, Redacted]
        public string ClientSecret { get; set; }

        [Required] public string InternalStsUrl { get; set; }
    }
}