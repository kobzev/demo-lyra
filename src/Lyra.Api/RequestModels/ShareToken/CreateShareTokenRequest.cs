namespace Lyra.Api.RequestModels.ShareToken
{
    using System.ComponentModel.DataAnnotations;

    public class CreateShareTokenRequest
    {
        [Required]
        public string Name { get; set; }
        [Required]
        public string ProductId { get; set; }
        [Required]
        public string Ticker { get; set; }
        [Required]
        public string DocumentUrl { get; set; }
        [Required]
        public int NumberOfDecimalPlaces { get; set; }

        public string Color { get; set; }
        public string InstrumentId { get; set; }
        public bool IsDeployed { get; set; }
        public bool IsFrozen { get; set; }
        public bool IsMinted { get; set; }
        public decimal TotalSupply { get; set; }

        public string ExternalAssetId { get; set; }
    }
}
