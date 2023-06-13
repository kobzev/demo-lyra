namespace Lyra.Persistence
{
    using Amazon.DynamoDBv2.DataModel;

    public static partial class Schema
    {
        public class ShareToken : Product
        {
            public static class Attributes
            {
                public const string Name = "share_token.name";
                public const string Ticker = "share_token.ticker";
                public const string DocumentUrl = "share_token.document_url";
                public const string IsDeployed = "share_token.is_deployed";
                public const string IsFrozen = "share_token.is_frozen";
                public const string BlockchainErrorMessage = "share_token.blockchain_error_message";
                public const string TotalSupply = "share_token.total_supply";
                public const string ExternalAssetId = "share_token.external_assetId";
            }

            [DynamoDBProperty(Attributes.Name)]
            public string Name { get; set; }

            [DynamoDBProperty(Attributes.Ticker)]
            public string Ticker { get; set; }

            [DynamoDBProperty(Attributes.DocumentUrl)]
            public string DocumentUrl { get; set; }

            [DynamoDBProperty(Attributes.IsDeployed)]
            public bool IsDeployed { get; set; }

            [DynamoDBProperty(Attributes.IsFrozen)]
            public bool IsFrozen { get; set; }

            [DynamoDBProperty(Attributes.BlockchainErrorMessage)]
            public string BlockchainErrorMessage { get; set; }

            [DynamoDBProperty(Attributes.TotalSupply)]
            public decimal TotalSupply { get; set; }

            [DynamoDBProperty(Attributes.ExternalAssetId)]
            public string ExternalAssetId { get; set; }
        }
    }
}
