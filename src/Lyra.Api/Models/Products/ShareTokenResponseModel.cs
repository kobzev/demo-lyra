namespace Lyra.Api.Models.Products
{
    using Lyra.Products;

    public class ShareTokenResponseModel : ProductModel
    {
        public string TenantId { get; set; }
        public string Name { get; set; }
        public string Ticker { get; set; }
        public string DocumentUrl { get; set; }
        public string Color { get; set; }
        public bool IsDeployed { get; set; }
        public bool IsFrozen { get; set; }

        public decimal TotalSupply { get; set; }

        public string BlockchainErrorMessage { get; set; }

        public string ExternalAssetId { get; set; }
    }
}
