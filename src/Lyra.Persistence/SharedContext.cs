namespace Alexandria.Persistence
{
    using System;
    using System.Collections.Concurrent;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;

    public sealed class SharedContext : IDisposable
    {
        private readonly ConcurrentDictionary<IAmazonDynamoDB, IDynamoDBContext> map = new ConcurrentDictionary<IAmazonDynamoDB, IDynamoDBContext>();

        private bool disposed;

        private SharedContext()
        {
        }

        public static SharedContext Instance = new SharedContext();

        public IDynamoDBContext GetSharedDynamoDBContext(IAmazonDynamoDB client) => this.map.GetOrAdd(client, c => new DynamoDBContext(client));

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            foreach (var mapping in map)
            {
                mapping.Value.Dispose();
            }

            this.disposed = true;
        }
    }
}
