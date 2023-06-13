namespace Lyra.Persistence
{
    using Amazon.DynamoDBv2.DataModel;

    public static partial class Schema
    {
        public class Product
        {
            public const string PartitionKeyPrefix = "PRODUCT";

            public static class CommonAttributes
            {
                public const string ProductPartitionKey = "product.partition_key";
                public const string ProductSortKey = "product.sort_key";

                public const string Category = "product.category";
                public const string Id = "product.id";
                public const string InstrumentId = "product.instrument_id";
                public const string Color = "product.color";
                public const string IsMinted = "product.is_minted";

                public const string ProductStatus = "product.status";
                public const string ProductStatusPartitionKey = "product.status.partition_key";
                public const string ProductStatusSortKey = "product.status.sort_key";
            }

            public static string GetPrimaryPartitionKey(string tenantId, string id) => $"TENANT#{tenantId}#{PartitionKeyPrefix}#{id.Trim().ToUpperInvariant()}";
            public static string GetPrimarySortKey(string category) => category.ToUpper();

            public static string GetProductPartitionKey(string tenantId) => $"TENANT#{tenantId}";
            public static string GetProductSortKey(string category, string productId) => $"{category.ToUpper()}#{productId}";

            public static string GetProductStatusPartitionKey(string tenantId, int status) => $"TENANT#{tenantId}#{status}";
            public static string GetProductStatusSortKey(string category, string productId) => $"{category.ToUpper()}#{productId}";


            [DynamoDBHashKey(Schema.Attributes.PartitionKey)]
            public string PrimaryPartitionKey { get; set; }

            [DynamoDBRangeKey(Schema.Attributes.SortKey)]
            public string PrimarySortKey { get; set; }

            [DynamoDBProperty(Product.CommonAttributes.Category)]
            public string Category { get; set; }

            [DynamoDBProperty(Product.CommonAttributes.Id)]
            public string Id { get; set; }

            [DynamoDBProperty(Product.CommonAttributes.Category)]
            public string Type { get; set; }

            [DynamoDBProperty(CommonAttributes.Color)]
            public string Color { get; set; }

            [DynamoDBProperty(CommonAttributes.IsMinted)]
            public string IsMinted { get; set; }

            [DynamoDBProperty(CommonAttributes.ProductPartitionKey)]
            public string ProductPartitionKey { get; set; }

            [DynamoDBProperty(CommonAttributes.ProductSortKey)]
            public string ProductSortKey { get; set; }

            [DynamoDBProperty(CommonAttributes.InstrumentId)]
            public string InstrumentId { get; set; }

            [DynamoDBProperty(CommonAttributes.ProductStatusPartitionKey)]
            public string ProductStatusPartitionKey { get; set; }

            [DynamoDBProperty(CommonAttributes.ProductStatusSortKey)]
            public string ProductStatusSortKey { get; set; }

            [DynamoDBProperty(CommonAttributes.ProductStatus)]
            public int ProductStatus { get; set; }
        }
    }
}
