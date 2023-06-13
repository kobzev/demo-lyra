namespace Lyra.Persistence
{
    using Amazon.DynamoDBv2.DataModel;

    public static partial class Schema
    {
        public class Crypto : Product
        {
            public static class Attributes
            {
                public const string ExternalAssetId = "crypto.external_assetId";
            }

            [DynamoDBProperty(Attributes.ExternalAssetId)]
            public string ExternalAssetId { get; set; }
        }
    }
}
