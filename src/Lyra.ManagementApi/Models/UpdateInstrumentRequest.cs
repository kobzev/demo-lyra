namespace Lyra.ManagementApi.Models
{
    public class UpdateInstrumentRequest
    {
        /// <summary>
        /// Get or Set the Instrument Name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Get or Set the Numer of Decimal Places
        /// </summary>
        public int NumberOfDecimalPlaces { get; set; }
    }
}
