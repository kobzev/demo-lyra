namespace Lyra.Persistence
{
    public static class Table
    {
        public const string Products = "lyra-products-v2";

        public static class Indicies
        {
            public const string CopyrightTokensByCreatorId = "copyright_tokens_by_creator_id";
            public const string CopyrightTokensBySecondaryMarketAvailability = "copyright_tokens_by_secondary_market_availability";
            public const string CopyrightTokensByExternalMusicId = "copyright_tokens_by_external_music_id";
            public const string Products = "products";
            public const string CopyrightTokensByTradingVolume = "copyright_tokens_by_trading_volume";
            public static string Instruments = "instruments";
            public const string ProductStatus = "product_status";
            public const string InstrumentStatus = "instrument_status";
        }
    }
}
