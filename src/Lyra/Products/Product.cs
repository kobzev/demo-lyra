namespace Lyra.Products
{
    using System;

    public class Product
    {
        public Product()
        {

        }
        public Product(string productId, string category, string color)
        {
            _ = productId ?? throw new ArgumentNullException(nameof(productId));

            var components = productId.Split('.');
            if (components.Length != 3)
            {
                throw new ArgumentException($"Invalid product ID: '{productId}'.", nameof(productId));
            }
            else
            {
                if (string.IsNullOrWhiteSpace(components[0].Trim()) || string.IsNullOrWhiteSpace(components[1].Trim()) || string.IsNullOrWhiteSpace(components[2].Trim()))
                {
                    throw new ArgumentException($"Invalid product ID: '{productId}'.", nameof(productId));
                }
                Type = components[0].ToLowerInvariant();
                SubType = components[1].ToLowerInvariant();
                Symbol = components[2].ToUpperInvariant();
            }
            //this.ProductId = productId;

            InstrumentId = productId;
            Category = category;
            Color = color;
        }

        public string Type { get; }
        public string SubType { get; }
        public string Symbol { get; }

        public string ProductId => this.ToString();
        public string InstrumentId { get; set; }
        public string Color { get; set; }
        public string Category { get; }
        public bool IsMinted { get; private set; }
        public int ProductStatus { get; set; }

        public override string ToString() => string.Join('.', Type, SubType, Symbol);

        public void SetAsMinted()
        {
            if (this.IsMinted) throw new InvalidOperationException("Product is already minted!");
            this.IsMinted = true;
        }

        public void SetAsNotMinted()
        {
            if (this.IsMinted)
                this.IsMinted = false;
        }
    }
}