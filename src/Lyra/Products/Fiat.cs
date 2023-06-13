namespace Lyra.Products
{
    using System;

    public class Fiat : Product
    {
        public Fiat(string productId, string color)
            : base(productId, ProductTypes.Fiat, color)
        {

            if (string.IsNullOrWhiteSpace(productId)) throw new ArgumentNullException(nameof(productId));
            if (string.IsNullOrWhiteSpace(color)) Color = string.Empty; // Keep Optional
        }
        public static Fiat RestoreFromDatabase(string productId, string color, string instrumentId, int productStatus)
        {
            var token = new Fiat(productId, color)
            {
                ProductStatus = productStatus,
                InstrumentId = instrumentId,
            };

            return token;
        }

    }
}
