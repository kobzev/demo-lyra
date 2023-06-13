namespace Lyra.Instruments.Models.Instruments
{
    public class InstrumentModel
    {
        public string Id { set; get; }
        public string Name { set; get; }
        public int NumberOfDecimalPlaces { get; set; }

        public int InstrumentStatus { get; set; }
    }
}
