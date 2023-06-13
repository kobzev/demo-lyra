namespace Lyra.Repository
{
    using System.Threading.Tasks;
    using Lyra.Instruments;
    using Lyra.Products;

    public interface IProductWriteRepository
    {
        // Add
        Task AddInstrumentAsync(string tenant, Instrument instrument);
        Task AddShareTokenProductWithInstrumentAsync(string tenant, ShareToken shareToken, Instrument instrument = null);
        Task AddShareTokenProductAsync(string tenant, ShareToken shareToken);
        Task AddCopyrightTokenWithInstrumentAsync(string tenant, CopyrightToken copyrightToken, Instrument instrument);
        Task AddCryptoProductWithInstrumentAsync(string tenant, Crypto crypto, Instrument instrument);
        Task AddCryptoProductAsync(string tenant, Crypto crypto);
        Task AddFiatProductAsync(string tenant, Fiat fiat);
        Task AddSimpleProductAsync(string tenant, Simple simple);
        Task AddSimpleProductWithInstrumentAsync(string tenant,Simple simple,Instrument instrument);
        Task AddFiatProductWithInstrumentAsync(string tenant, Fiat fiat,Instrument instrument);

        // Update
        Task UpdateFiatTokenAsync(string tenant, Fiat fiat);
        Task UpdateSimpleTokenAsync(string tenant, Simple simple);
        Task UpdateCopyrightTokenAsync(string tenant, CopyrightToken copyrightToken);
        Task UpdateCryptoTokenAsync(string tenant, Crypto crypto);
        Task UpdateShareTokenAsync(string tenant, ShareToken request, string idempotencyToken = null);
        Task UpdateProductAsMinted(string tenant, Product product);
        Task UpdateProductStatusAsync(string tenant, Product product, int productStatus);
        Task UpdateInstrumentStatusAsync(string tenant, string id, int instrumentStatus);
        Task UpdateInstrumentAsync(string tenant, string id, Instrument request);

        // Remove
        Task RemoveProductWithInstrumentAsync(string tenant, string id, string productType);
        Task RemoveProductAsync(string tenant, string id, string productType);
        Task RemoveInstrumentAsync(string tenant, string id);
    }
}