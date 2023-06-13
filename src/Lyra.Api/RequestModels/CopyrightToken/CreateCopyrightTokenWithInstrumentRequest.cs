namespace Lyra.Api.Models.Products
{
    using System.ComponentModel.DataAnnotations;

    public class CreateCopyrightTokenWithInstrumentRequest
    {
        [Required]
        public CreateInstrumentRequest Instrument { set; get; }
        
        [Required]
        public CreateCopyrightTokenRequest CopyrightToken { set; get; }
    }
    
    public class CreateInstrumentRequest
    {
        [Required]
        public string Name { get; set; }

        public int? NumberOfDecimalPlaces { get; set; }
    }

    public class CreateCopyrightTokenRequest
    {
        [Required]
        public SubTypeModel SubType { get; set; }
        [Required]
        public string ExternalMusicId { get; set; }
        [Required]
        public string CreatorId { set; get; }
        public string Color { get; set; }
        public string Icon { get; set; }
        [Required]
        public string Ownership { get; set; }
        [Required]
        public decimal Amount { get; set; }
        [Required]
        public CreateSongDetailsModel SongDetails { set; get; }
        public decimal TradingVolume { get; set; } = 0;
    }

    public enum SubTypeModel
    {
        Golden,
        Diamond
    }
}