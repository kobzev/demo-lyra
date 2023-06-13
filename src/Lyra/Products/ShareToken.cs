namespace Lyra.Products
{
    public class ShareToken : Product
    {
        public string Name { get; set; }

        public string Ticker { get; set; }

        public string DocumentUrl { get; set; }

        public bool IsDeployed { get; set; }

        public bool IsFrozen { get; set; }

        /// <summary>Total supply - all wallets and addresses combined</summary>
        public decimal TotalSupply { get; set; }

        public string BlockchainErrorMessage { set; get; }

        public int NumberOfDecimalPlaces { set; get; }

        public string ExternalAssetId { get; set; }

        public ShareToken(string productId, string name, string ticker, string documentUrl, bool isMinted, string color, int numberOfDecimalPlaces = 0)
            : base(productId, ProductTypes.ShareToken, color)
        {
            this.Name = name;
            this.Ticker = ticker;
            this.DocumentUrl = documentUrl;
            this.NumberOfDecimalPlaces = numberOfDecimalPlaces; //We do not pass anything from Jarvis at the moment
            if (isMinted) this.SetAsMinted();
        }

        public static ShareToken RestoreFromDatabase(string productId, string name, string ticker, string documentUrl,
            bool isMinted, string color, bool isDeployed, bool isFrozen,
            string blockchainErrorMessage, decimal totalSupply, string instrumentId, int productStatus, string externalAssetId)
        {
            var token = new ShareToken(productId, name, ticker, documentUrl, isMinted, color)
            {
                IsDeployed = isDeployed,
                IsFrozen = isFrozen,
                BlockchainErrorMessage = blockchainErrorMessage,
                TotalSupply = totalSupply,
                ProductStatus = productStatus,
                InstrumentId = instrumentId,
                ExternalAssetId= externalAssetId
            };
            return token;
        }
    }
}
