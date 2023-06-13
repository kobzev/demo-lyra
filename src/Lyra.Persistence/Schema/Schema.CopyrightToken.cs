namespace Lyra.Persistence
{
    using Amazon.DynamoDBv2.DataModel;
    using Lyra.Products;

    public static partial class Schema
    {
        public class CopyrightToken : Product
        {
            public static class Attributes
            {
                public const string ExternalMusicIdPartitionKey = "copyright_token.external_music_id_partition_key";
                public const string ExternalMusicIdSortKey = "copyright_token.external_music_id_sort_key";
                
                public const string CreatorIdPartitionKey = "copyright_token.creator_id_partition_key";
                public const string CreatorIdSortKey = "copyright_token.creator_id_sort_key";
                
                public const string SecondaryMarketAvailabilityPartitionKey = "copyright_token.secondary_market_avilability_partition_key";
                public const string SecondaryMarketAvailabilitySortKey = "copyright_token.secondary_market_avilability_sort_key";
                public const string SecondaryMarketAvailabilityTradingVolumeSortKey = "copyright_token.secondary_market_availability_trading_volume_sort_key";

                public const string ExternalMusicId = "copyright_token.external_music_id";
                public const string Icon = "copyright_token.icon";
                public const string CreatorId = "copyright_token.creator_id";
                public const string SubType = "copyright_token.sub_type";
                public const string Ownership = "copyright_token.ownership";
                public const string Amount = "copyright_token.amount";
                public const string AlreadyAuctionedAmount = "copyright_token.already_auctioned_amount";
                public const string SongDetails = "copyright_token.song_details";
                public const string IsAvailableForSecondaryMarket = "copyright_token.is_available_for_secondary_market";
                public const string TradingVolume = "copyright_token.trading_volume";
            }
            
            public static string GetExternalMusicIdPartitionKey(string tenantId, string externalMusicId) => $"TENANT#{tenantId}#EXTERNAL_MUSIC_ID#{externalMusicId}";
            public static string GetExternalMusicIdSortKey(string externalMusicId, SubType subType) => $"EXTERNAL_MUSIC_ID#{externalMusicId}#SUB_TYPE#{subType.ToString().ToUpper()}";
            
            
            public static string GetSecondaryMarketAvailabilityPartitionKey(string tenantId, bool isAvailable) => $"TENANT#{tenantId}#SECONDARY_MARKET_AVAILABILITY#IS_AVAILABLE#{isAvailable.ToString().ToUpper()}";
            public static string GetSecondaryMarketAvailabilitySortKey(string productId) => $"SECONDARY_MARKET_AVAILABILITY#PRODUCT#{productId}";
            public static string GetSecondaryMarketAvailabilityTradingVolumeSortKey(decimal tradingVolume) => $"SECONDARY_MARKET_AVAILABILITY#TRADING_VOLUME#{tradingVolume}";
            
            public static string GetCreatorIdPartitionKey(string tenantId, string creatorId) => $"TENANT#{tenantId}#CREATOR_ID#{creatorId}";
            public static string GetCreatorIdSortKey(string creatorId, string productId) => $"CREATOR_ID#{creatorId}#PRODUCT_ID#{productId}";
            

            [DynamoDBProperty(Attributes.SecondaryMarketAvailabilityPartitionKey)]
            public string SecondaryMarketAvailabilityPartitionKey { set; get; }
            
            [DynamoDBProperty(Attributes.SecondaryMarketAvailabilitySortKey)]
            public string SecondaryMarketAvailabilitySortKey { set; get; }
         
            [DynamoDBProperty(Attributes.SecondaryMarketAvailabilityTradingVolumeSortKey)]
            public decimal SecondaryMarketAvailabilityTradingVolumeSortKey { set; get; }
            
            [DynamoDBProperty(Attributes.ExternalMusicIdPartitionKey)]
            public string ExternalMusicIdPartitionKey { set; get; }
            
            [DynamoDBProperty(Attributes.ExternalMusicIdSortKey)]
            public string ExternalMusicIdSortKey { set; get; }
            
            [DynamoDBProperty(Attributes.CreatorIdPartitionKey)]
            public string CreatorIdPartitionKey { set; get; }
            
            [DynamoDBProperty(Attributes.CreatorIdSortKey)]
            public string CreatorIdSortKey { set; get; }

            [DynamoDBProperty(Attributes.ExternalMusicId)]
            public string ExternalMusicId { get; set; }

            [DynamoDBProperty(Attributes.Icon)]
            public string Icon { get; set; }

            [DynamoDBProperty(Attributes.CreatorId)]
            public string CreatorId { get; set; }

            [DynamoDBProperty(Attributes.SubType)]
            public string SubType { get; set; }

            [DynamoDBProperty(Attributes.Ownership)]
            public string Ownership { get; set; }

            [DynamoDBProperty(Attributes.Amount)]
            public decimal Amount { get; set; }

            [DynamoDBProperty(Attributes.AlreadyAuctionedAmount)]
            public decimal AlreadyAuctionedAmount { get; set; }

            [DynamoDBProperty(Attributes.IsAvailableForSecondaryMarket)]
            public decimal IsAvailableForSecondaryMarket { get; set; }
            
            [DynamoDBProperty(CommonAttributes.IsMinted)]
            public bool IsMinted { get; set; }
            
            [DynamoDBProperty(Attributes.TradingVolume)]
            public decimal TradingVolume { get; set; }

            [DynamoDBProperty(Attributes.SongDetails)]
            public string SongDetails { get; set; }
        }
    }
}
