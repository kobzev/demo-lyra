namespace Lyra.ManagementApi.Models
{
    using System;
    using System.Threading.Tasks;
    using Lyra.Api.Models.Products;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public class ProductsResponseFactory
    {
        private readonly IProductReadRepository productReadRepository;

        private readonly ManagementApiConfiguration configuration;
        private readonly ILogger<ProductsResponseFactory> logger;

        public ProductsResponseFactory(IProductReadRepository productReadRepository, ILogger<ProductsResponseFactory> logger,
            ManagementApiConfiguration configuration)
        {
            this.configuration = configuration;
            this.logger = logger;
            this.productReadRepository = productReadRepository;
        }

        public async Task<ProductModel> MapProduct(string tenantId, Product product)
        {
            try
            {
                var instrument = await this.productReadRepository.GetInstrumentAsync(tenantId, product.InstrumentId);

                if (instrument == null)
                {
                    logger.LogDebug($"instruments not found with instrument id {product.InstrumentId} for tenant {tenantId}");
                }

                if (product is Crypto crypto)
                {
                    return new CryptoResponseModel
                    {
                        Id = crypto.ProductId,
                        Category = crypto.Category,
                        Code = instrument?.Symbol,
                        Name = instrument?.Name,
                        Icon = instrument?.Name.Replace(" ", ""),
                        Card = CloudFrontUrl(configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-card.png"),
                        Cover = CloudFrontUrl(configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-cover.png"),
                        Color = crypto.Color,
                        InstrumentId = product.InstrumentId,
                        ProductStatus = product.ProductStatus,
                        IsMinted = crypto.IsMinted,
                        ExternalAssetId = crypto.ExternalAssetId,
                    };
                }
                else if (product is Simple simple)
                {
                    return new SimpleResponseModel
                    {
                        Id = simple.ProductId,
                        Category = simple.Category,
                        Code = instrument?.Symbol,
                        Name = instrument?.Name,
                        Icon = instrument?.Name.Replace(" ", ""),
                        Card = CloudFrontUrl(configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-card.png"),
                        Cover = CloudFrontUrl(configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-cover.png"),
                        Color = simple.Color,
                        InstrumentId = product.InstrumentId,
                        ProductStatus = product.ProductStatus,
                    };
                }

                else if (product is Fiat fiat)
                {
                    return new FiatResponseModel
                    {
                        Id = fiat.ProductId,
                        Category = fiat.Category,
                        Code = instrument?.Symbol,
                        Name = instrument?.Name,
                        Icon = instrument?.Name.Replace(" ", ""),
                        Card = CloudFrontUrl(configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-card.png"),
                        Cover = CloudFrontUrl(configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-cover.png"),
                        Color = fiat.Color,
                        InstrumentId = product.InstrumentId,
                        ProductStatus = product.ProductStatus,
                    };
                }
                else if (product is ShareToken shareToken)
                {
                    //Add Misising Number of Decimal Palces
                    if (instrument != null)
                    {
                        shareToken.NumberOfDecimalPlaces = instrument.NumberOfDecimalPlaces;
                    }
                    return MapShareToken(tenantId, shareToken);
                }

                return new ProductModel
                {
                    Id = product.ProductId,
                    Category = product.Category,
                    InstrumentId = product.InstrumentId,
                    ProductStatus = product.ProductStatus,
                    IsMinted = product.IsMinted,
                };
            }
            catch (Exception ex)
            {
                var dynamoAttributes = JsonConvert.SerializeObject(product);
                logger.LogError("Exception ocurred and the message is  {Exception} and the serialized message is {Attribute}", ex, dynamoAttributes);
                throw;
            }
        }

        private static string CloudFrontUrl(string cloudFrontUrl, string end)
        {
            return cloudFrontUrl + "misc/products/" + end;
        }

        public ShareTokenResponseModel MapShareToken(string tenantId, ShareToken shareToken)
        {
            return new ShareTokenResponseModel
            {
                Id = shareToken.ProductId,
                Category = shareToken.Category,
                IsMinted = shareToken.IsMinted,
                TenantId = tenantId,
                Name = shareToken.Name,
                Ticker = shareToken.Ticker,
                DocumentUrl = shareToken.DocumentUrl,
                Color = shareToken.Color,
                IsDeployed = shareToken.IsDeployed,
                IsFrozen = shareToken.IsFrozen,
                TotalSupply = shareToken.TotalSupply,
                BlockchainErrorMessage = shareToken.BlockchainErrorMessage,
                InstrumentId = shareToken.InstrumentId,
                ProductStatus = shareToken.ProductStatus,
                NumberOfDecimalPlaces = shareToken.NumberOfDecimalPlaces,
                ExternalAssetId = shareToken.ExternalAssetId,
            };
        }
    }
}
