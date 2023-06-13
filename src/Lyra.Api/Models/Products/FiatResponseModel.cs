namespace Lyra.Api.Models.Products
{
    public class FiatResponseModel : ProductModel
    {
        public string Code { set; get; }
        public string Name { set; get; }
        public string Icon { set; get; }
        public string Card { set; get; }
        public string Cover { set; get; }
        public string Color { set; get; }
    }
}
