using System.ComponentModel.DataAnnotations;

namespace Lyra.ManagementApi.Models
{
    public class CreateCryptoRequest
    {
        public string ProductId { get; set; }

        public string Color { get; set; }

        public bool IsMinted { get; set; }

        public string InstrumentId { get; set; }

        public string ExternalAssetId { get; set; }
    }
}
