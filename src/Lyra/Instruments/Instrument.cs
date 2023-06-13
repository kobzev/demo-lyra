namespace Lyra.Instruments
{
    using System;

    public class Instrument
    {
        public Instrument(string id, string name, int instrumentStatus = 0, int numberOfDecimalPlaces = 0)
        {
            _ = id ?? throw new ArgumentNullException(nameof(id));
            _ = name ?? throw new ArgumentNullException(nameof(name));

            var components = id.Split('.');
            if (components.Length < 2 || components.Length > 3)
            {
                throw new ArgumentException($"Invalid instrument ID: '{id}'.", nameof(id));
            }

            this.Type = components[0].ToLowerInvariant();
            if (string.IsNullOrWhiteSpace(this.Type))
            {
                throw new ArgumentException($"Invalid instrument ID: '{id}'.", nameof(id));
            }

            var next = components[1];
            if (string.IsNullOrWhiteSpace(next))
            {
                throw new ArgumentException($"Invalid instrument ID: '{id}'.", nameof(id));
            }

            if (components.Length == 2)
            {
                this.Symbol = next.ToUpperInvariant();
            }
            else
            {
                this.SubType = next.ToLowerInvariant();
                this.Symbol = components[2].ToUpperInvariant();
                if (string.IsNullOrWhiteSpace(this.Symbol))
                {
                    throw new ArgumentException($"Invalid instrument ID: '{id}'.", nameof(id));
                }
            }

            Name = name;

            this.NumberOfDecimalPlaces = numberOfDecimalPlaces;
            this.InstrumentStatus = instrumentStatus;
        }

        public string Id => this.ToString();

        public string Name { get; set; }

        public string Type { get; }

        public string SubType { get; }

        public string Symbol { get; }

        // seem to be useful
        public string Url => string.Join('-', this.Type, this.SubType, this.Symbol).ToLowerInvariant();

        public string Category => string.Join(".", this.Type, this.SubType);

        public bool IsCrypto => this.Category == "currency.crypto";

        public bool IsSimple => this.Category == "currency.simple";

        public bool IsToken => this.Type == "token";

        public bool Equals(Instrument other) => this.Type == other?.Type && this.SubType == other?.SubType && this.Symbol == other?.Symbol;

        public override bool Equals(object obj) => obj is Instrument instrument && this.Equals(instrument);

        public override int GetHashCode() => HashCode.Combine(this.Type, this.SubType, this.Symbol);

        public static bool operator ==(Instrument x, Instrument y) => x is object && x.Equals(y) || x is null && y is null;

        public static bool operator !=(Instrument x, Instrument y) => !(x == y);

        public override string ToString() => string.IsNullOrEmpty(this.SubType) ? string.Join('.', this.Type, this.Symbol) : string.Join('.', this.Type, this.SubType, this.Symbol);

        public static implicit operator string(in Instrument instrument) => instrument?.ToString();
        public int NumberOfDecimalPlaces { get; set; }

        public static Instrument RestoreFromDatabase(string id, string name, int numberOfDecimalPlaces, int instrumentStatus)
        {
            return new Instrument(id, name, instrumentStatus,numberOfDecimalPlaces);
        }
        public int InstrumentStatus { get; set; }

    }

}
