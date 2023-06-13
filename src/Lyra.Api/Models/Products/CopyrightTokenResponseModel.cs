namespace Lyra.Api.Models.Products
{
    public class CopyrightTokenResponseModel : ProductModel
    {
        public string CreatorId { get; set; }
        public string Card { set; get; }
        public string Cover { set; get; }
        public string Color { set; get; }
        public bool IsAvailableForSecondaryMarket { set; get; }
        public string ExternalMusicId { get; set; }
        public string Icon { get; set; }
        public SubTypeModel SubType { get; set; }
        public string Ownership { get; set; }
        public decimal Amount { get; set; }
        public decimal AlreadyAuctionedAmount { get; set; }
        
        public decimal TradingVolume { get; set; }
        public SongDetailsModel SongDetails { set; get; }
    }
}
