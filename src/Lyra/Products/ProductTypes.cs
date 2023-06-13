namespace Lyra.Products
{
    using System;
    using System.Collections.Generic;

    public static class ProductTypes
    {
        public const string Crypto = "Crypto";
        public const string CopyrightToken = "CopyrightToken";
        public const string ShareToken = "ShareToken";
        public const string Simple = "Simple";
        public const string Fiat = "Fiat";

        public static string[] Products = new[] { Crypto, CopyrightToken, ShareToken, Simple, Fiat };

        private static readonly Dictionary<Type, string> TypesMap = new Dictionary<Type, string>()
        {
            { typeof(Crypto), Crypto },
            { typeof(CopyrightToken), CopyrightToken },
            { typeof(ShareToken), ShareToken },
            { typeof(Simple), Simple },
            { typeof(Fiat), Fiat },
        };
    }
}
