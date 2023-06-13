namespace Lyra.Products
{
    using System;

    public class Simple : Product
    {
        public Simple(string productId, string color)
            : base(productId, ProductTypes.Simple, color)
        {

            if (string.IsNullOrWhiteSpace(productId)) throw new ArgumentNullException(nameof(productId));
            if (string.IsNullOrWhiteSpace(color)) Color = string.Empty; // Keep Optional
        }
        public static Simple RestoreFromDatabase(string productId, string color, string instrumentId, int productStatus)
        {
            var token = new Simple(productId, color)
            {
                ProductStatus = productStatus,
                InstrumentId = instrumentId,
            };

            return token;
        }

    }
}
