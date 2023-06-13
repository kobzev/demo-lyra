namespace Lyra.Api.Models.Products
{
    public class CopyrightTokenResponseOldModel : ProductModel
    {
        public string Code { get; set; }

        public string Name { get; set; }

        public string Icon { get; set; }

        public string Card { get; set; }

        public string Cover { get; set; }

        public string ExternalMusicId { get; set; }

        public string Color { get; set; }

        public bool Disabled { get; set; }

        public string ArtistName { get; set; }

        public string AlbumName { get; set; }

        public decimal AllTimeEarnings { get; set; }

        public decimal WeeklyEarned { get; set; }

        public decimal UnitsAvailable { get; set; }
        
        public decimal Amount { get; set; }
        
        public decimal AlreadyAuctionedAmount { get; set; }

        public string AuctionCloseDate { get; set; }

        public string Description { get; set; }
    }
}
