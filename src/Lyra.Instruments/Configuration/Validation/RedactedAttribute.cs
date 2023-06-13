namespace Lyra.Instruments.Configuration.Validation
{
    using System;

    public class RedactedAttribute : Attribute
    {
        public static readonly string[] LikelyRedactedKeywords = new[]
        {
            "secret",
            "token",
            "password"
        };

    }
}
