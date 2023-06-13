namespace Lyra.Persistence
{
    using System;
    using System.Globalization;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.Util;

    public static partial class Schema
    {
        public static class Attributes
        {
            public const string PartitionKey = "partition_key";
            public const string SortKey = "sort_key";
        }

        public static class DateTimeConverter
        {
            public sealed class ISO8601DateFormat : IPropertyConverter
            {
                public object FromEntry(DynamoDBEntry entry) => TryGetPrimitive(entry, out var primitive) ? (object)FromString(primitive.AsString()) : null;

                public DynamoDBEntry ToEntry(object value) => throw new NotImplementedException();

                internal static DateTime FromString(string value) =>
                    DateTime.ParseExact(value, AWSSDKUtils.ISO8601DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
            }

            public sealed class RoundtripDateFormat : IPropertyConverter
            {
                public object FromEntry(DynamoDBEntry entry) => TryGetPrimitive(entry, out var primitive) ? (object)FromString(primitive.AsString()) : null;

                public DynamoDBEntry ToEntry(object value) => throw new NotImplementedException();

                internal static DateTime FromString(string value) => DateTime.ParseExact(value, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            }

            private static bool TryGetPrimitive(DynamoDBEntry entry, out Primitive primitive)
            {
                primitive = entry as Primitive;
                if (primitive == null)
                {
                    return false;
                }

                if (primitive.Type != DynamoDBEntryType.String)
                {
                    throw new InvalidCastException();
                }

                return true;
            }
        }

    }
}
