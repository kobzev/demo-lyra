namespace Lyra.Api.Models.Products
{
    public class SongDetailsModel
    {
        public string Name { set; get; }
        public string ArtistName { get; set; }
        public string AlbumName { get; set; }
        public string Description { get; set; }
        public string Genre { set; get; }
        public decimal MiningByStreaming { set; get; }
        public decimal MiningByCuration { set; get; }
        public ContributorsModel Contributors { set; get; }
    }
}
