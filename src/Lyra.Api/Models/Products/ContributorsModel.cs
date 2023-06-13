namespace Lyra.Api.Models.Products
{
    using System.Collections.Generic;

    public class ContributorsModel
    {
        public List<ContributorModel> Owners { set; get; }
        public List<ContributorModel> Songwriters { set; get; }
        public List<ContributorModel> Producers { set; get; }
        public List<ContributorModel> Engineers { set; get; }
        public List<ContributorModel> Composers { set; get; }
        public List<ContributorModel> Lyricists { set; get; }
        public List<ContributorModel> FeaturedArtists { set; get; }
        public List<ContributorModel> NonFeaturedMusicians { set; get; }
        public List<ContributorModel> NonFeaturedVocalists { set; get; }
    }
}
