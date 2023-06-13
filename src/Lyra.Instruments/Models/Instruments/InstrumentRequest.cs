namespace Lyra.Instruments.Models.Instruments
{
    public class InstrumentRequest
    {
        public string TenantId { set; get; }

        public string InstrumentId { set; get; }

        public int InstrumentStatus { get; set; }
    }
}
