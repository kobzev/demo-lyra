namespace Lyra.Persistence
{
    using System;
    using System.Text;
    using LykkeCorp.DynamoDBv2.Pagination;

    public sealed class PaginationTokenWrapper
    {
        public const string ForwardsToken = "eyJkaXJlY3Rpb24iOiJmb3J3YXJkcyJ9";      // decoded: {"direction":"forwards"}
        public const string BackwardsToken = "eyJkaXJlY3Rpb24iOiJiYWNrd2FyZHMifQ=="; // decoded: {"direction":"backwards"}

        public PaginationTokenWrapper(string value)
        {
            _ = value ?? throw new ArgumentNullException(nameof(value));

            var components = value.Split('.', StringSplitOptions.None);
            if (components.Length == 1)
            {
                this.ReadForwards = IsReadForwards(components[0]);
            }
            else
            {
                this.InnerToken = components[0];
                this.ReadForwards = IsReadForwards(components[1]);
            }

            bool IsReadForwards(string encodedDirection)
            {
                if (string.Equals(encodedDirection, ForwardsToken, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                if (string.Equals(encodedDirection, BackwardsToken, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }

                throw new ArgumentException("Invalid pagination token value.", nameof(value));
            };
        }

        public PaginationTokenWrapper(bool readForwards)
        {
            this.ReadForwards = readForwards;
        }

        public PaginationTokenWrapper(bool readForwards, string innerToken)
        {
            this.ReadForwards = readForwards;
            this.InnerToken = innerToken ?? throw new ArgumentNullException(nameof(innerToken));
        }

        public PaginationTokenWrapper(bool readForwards, PaginationToken innerToken)
        {
            this.ReadForwards = readForwards;
            this.InnerToken = innerToken?.ToString();
        }

        public bool ReadForwards { get; }

        public string InnerToken { get; }

        public override string ToString()
        {
            var builder = new StringBuilder();

            if (!string.IsNullOrEmpty(this.InnerToken))
            {
                builder.Append(this.InnerToken);
                builder.Append('.');
            }

            builder.Append(this.ReadForwards ? ForwardsToken : BackwardsToken);

            return builder.ToString();
        }

        public static implicit operator string(in PaginationTokenWrapper paginationToken) => paginationToken?.ToString();
    }
}
