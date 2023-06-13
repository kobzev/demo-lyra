namespace Lyra.Api.Configuration.Validation
{
    public class ConfigurationError
    {
        public string ErrorMessage { get; set; }

        public string Property { get; set; }

        public override string ToString()
        {
            return $"{this.Property}- {this.ErrorMessage}";
        }
    }
}
