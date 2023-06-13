using Lyra.Instruments;

namespace Lyra.Api.Models.Instruments
{
    public class InstrumentModel
    {
        public string Id { set; get; }
        public string Name { set; get; }
        public int NumberOfDecimalPlaces { get; set; }

        public int InstrumentStatus { set; get; }
    }
}
