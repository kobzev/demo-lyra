namespace Lyra.Api.Models.Products
{
    public class CreateSongDetailsModel
    {
        public string Name { set; get; }
        public string ArtistName { get; set; }
        public string AlbumName { get; set; }
        public string Description { get; set; }
        public string Genre { set; get; }
        public decimal MiningByStreaming { set; get; }
        public decimal MiningByCuration { set; get; }
    }
}
