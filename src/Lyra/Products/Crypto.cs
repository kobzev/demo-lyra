namespace Lyra.Products
{
    using System;

    public class Crypto : Product
    {
        public Crypto(string productId, string color, bool isMinted = false)
            : base(productId, ProductTypes.Crypto, color)
        {

            if (string.IsNullOrWhiteSpace(productId)) throw new ArgumentNullException(nameof(productId));
            if (string.IsNullOrWhiteSpace(color)) Color = string.Empty;
            if (isMinted) this.SetAsMinted();
        }
        public string ExternalAssetId { get; set; }
        public static Crypto RestoreFromDatabase(string productId, string color, bool isMinted, string instrumentId, int productStatus, string externalAssetId)
        {
            var token = new Crypto(productId, color)
            {
                ProductStatus = productStatus,
                InstrumentId = instrumentId,
                ExternalAssetId = externalAssetId
            };

            if (isMinted)
                token.SetAsMinted();
            return token;
        }

    }
}
