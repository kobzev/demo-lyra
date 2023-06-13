namespace Lyra.Api.Models.Products
{
    public class ProductModel
    {
        public string Id { set; get; }
        public string Category { set; get; }
        public bool IsMinted { set; get; }
        public int NumberOfDecimalPlaces { get; set; }
        public string InstrumentId { get; set; }
        public int ProductStatus { set; get; }
    }
}