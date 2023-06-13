namespace Lyra.Repository
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Lyra.Instruments;
    using Lyra.Products;

    public interface IProductReadRepository
    {
        Task<IEnumerable<Product>> GetAllProductsAsync(string tenant);

        Task<ListResult<Product>> GetAllProductsPaginatedAsync(string tenant, string category, int pageSize, string paginationToken);

        Task<ListResult<Product>> GetAllEnabledProductsPaginatedAsync(string tenant, string category, int pageSize, string paginationToken);
        Task<IEnumerable<T>> GetAllProductsAsync<T>(string tenant) where T : Product;

        Task<Product> GetProductAsync(string tenant, string productId);

        Task<T> GetProductAsync<T>(string tenant, string productId) where T : Product;

        Task<IEnumerable<Instrument>> GetAllInstrumentsAsync(string tenant);

        Task<ListResult<Instrument>> GetAllInstrumentsPaginatedAsync(string tenantId, int pageSize, string token);
        Task<ListResult<Instrument>> GetAllEnabledInstrumentsPaginatedAsync(string tenant, int pageSize, string paginationToken);

        Task<Instrument> GetInstrumentAsync(string tenant, string instrumentId);

        Task<IEnumerable<CopyrightToken>> GetCopyrightTokensByCreatorIdAsync(string tenant, string creatorId);

        Task<IEnumerable<CopyrightToken>> GetCopyrightTokensByExternalMusicIdAsync(string tenant, string externalMusicId);

        Task<IEnumerable<CopyrightToken>> GetCopyrightTokensByExternalMusicIdAsync(string tenant, IEnumerable<string> externalMusicIds);

        Task<IEnumerable<CopyrightToken>> GetAvailableForSecondaryMarketAsync(string tenant);

        Task<IEnumerable<CopyrightToken>> GetAvailableForSecondaryMarketAsyncRankByTradingVolume(string tenant);
    }
}