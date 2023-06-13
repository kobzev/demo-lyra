namespace Lyra.Api.Models.Products
{
    using Lyra.Products;

    public class ContributorModel
    {
        public ContributorModel()
        {
            
        }

        public ContributorModel(Contributor contributor)
        {
            this.ProfileId = contributor.ProfileId;
            this.TrackingAccountId = contributor.TrackingAccountId;
            this.Email = contributor.Email;
            this.Percentage = contributor.Percentage;
        }
        
        public string ProfileId { set; get; }
        public string TrackingAccountId { set; get; }
        public string Email { set; get; }
        public decimal Percentage { set; get; }

        public Contributor ToDomain()
        {
            return new Contributor(this.ProfileId, this.TrackingAccountId, this.Email, this.Percentage);
        }
    }
}
