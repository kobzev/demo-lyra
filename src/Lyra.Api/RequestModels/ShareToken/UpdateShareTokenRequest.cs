namespace Lyra.Api.Models.Products
{
    public class UpdateShareTokenRequest
    {
        public string Id { get; set; }
        public bool? IsDeployed { get; set; }
        public bool? IsFrozen { get; set; }
        public bool? IsMinted { get; set; }
        public decimal? TotalSupply { set; get; }
        public string BlockchainErrorMessage { set; get; }
        public string Color { get; set; }
        public string DocumentUrl { get; set; }
        public string InstrumentId { get; set; }
        public string Name { get; set; }
        public int? NumberOfDecimalPlaces { get; set; }
        public string Ticker { get; set; }

        public string ExternalAssetId { get; set; }
    }
}
