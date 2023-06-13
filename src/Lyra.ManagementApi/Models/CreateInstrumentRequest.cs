namespace Lyra.ManagementApi.Models
{
    using System.ComponentModel.DataAnnotations;

    public class CreateInstrumentRequest
    {
        [Required]
        public string InstrumentId { get; set; }

        [Required]
        public string Name { get; set; }

        public int NumberOfDecimalPlaces { get; set; }

        public int InstrumentStatus { get; set; }
    }
}
