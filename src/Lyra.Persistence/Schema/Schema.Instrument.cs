namespace Lyra.Persistence
{
    using Amazon.DynamoDBv2.DataModel;

    public static partial class Schema
    {
        public class Instrument
        {
            public const string PartitionKeyPrefix = "INSTRUMENT";

            public static class Attributes
            {
                public const string Id = "instrument.id";
                public const string Name = "instrument.name";
                public const string InstrumentTenantPartitionKey = "instrument.partition_key";
                public const string InstrumentTenantSortKey = "instrument.sort_key";
                public const string NumberOfDecimalPlaces = "instrument.number_of_decimal_places";
                
                public const string InstrumentStatus = "instrument.status";
                public const string InstrumentStatusPartitionKey = "instrument.status.partition_key";
                public const string InstrumentStatusSortKey = "instrument.status.sort_key";
            }

            public static string GetPartitionKey(string tenantId, string id) => $"TENANT#{tenantId}#{PartitionKeyPrefix}#{id.Trim().ToUpperInvariant()}";
            public static string GetSortKey() => "INSTRUMENT";
            public static string GetTenantPartitionKey(string tenantId) => $"TENANT#{tenantId}";
            public static string GetTenantSortKey(string instrumentId)  => $"INSTRUMENT#{instrumentId}";

            public static string GetInstrumentStatusPartitionKey(string tenantId,int status) => $"TENANT#{tenantId}#{status}";
            public static string GetInstrumentStatusSortKey(string instrumentId) => $"INSTRUMENT#{instrumentId}";

            [DynamoDBHashKey(Schema.Attributes.PartitionKey)]
            public string PartitionKey { get; set; }

            [DynamoDBRangeKey(Schema.Attributes.SortKey)]
            public string SortKey { get; set; }

            [DynamoDBProperty(Attributes.Id)]
            public string Id { get; set; }

            [DynamoDBProperty(Attributes.Name)]
            public string Name { get; set; }
            
            [DynamoDBProperty(Attributes.NumberOfDecimalPlaces)]
            public string NumberOfDecimalPlaces { get; set; }
            
            [DynamoDBProperty(Attributes.InstrumentTenantPartitionKey)]
            public string InstrumentTenantPartitionKey { get; set; }
            
            [DynamoDBProperty(Attributes.InstrumentTenantSortKey)]
            public string InstrumentTenantSortKey { get; set; }

            [DynamoDBProperty(Attributes.InstrumentStatusPartitionKey)]
            public string InstrumentStatusPartitionKey { get; set; }

            [DynamoDBProperty(Attributes.InstrumentStatusSortKey)]
            public string InstrumentStatusSortKey { get; set; }

            [DynamoDBProperty(Attributes.InstrumentStatus)]
            public int InstrumentStatus { get; set; }
        }
    }
}
