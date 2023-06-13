namespace Lyra.ManagementApi.Controllers
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Lyra.Instruments;
    using Lyra.ManagementApi.Models;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Authorization;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    [Authorize(Infrastructure.Constants.SecurityPolicy)]
    public class CryptoController : ControllerBase
    {
        #region Declaration
        private readonly ILogger<CryptoController> _logger;
        private readonly IProductWriteRepository _productWriteRepository;
        private readonly IProductReadRepository _productReadRepository;
        private readonly ProductsResponseFactory _productsResponseFactory;
        #endregion

        #region Constructor
        public CryptoController(
            ILogger<CryptoController> logger,
            IProductReadRepository productReadRepository,
            IProductWriteRepository productWriteRepository,
            ProductsResponseFactory productsResponseFactory)
        {
            _productsResponseFactory = productsResponseFactory ?? throw new ArgumentNullException(nameof(productsResponseFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _productReadRepository = productReadRepository ?? throw new ArgumentNullException(nameof(productReadRepository));
            _productWriteRepository = productWriteRepository ?? throw new ArgumentNullException(nameof(productWriteRepository));
        }
        #endregion

        #region Public Methods - API
        // Note (Prashant): Could use GetProducts with a type query.
        [HttpGet("{tenantId}/products/cryptos")]
        public async Task<IActionResult> GetAll([FromRoute] string tenantId)
        {
            _logger.LogDebug($"Received request to get all crypto tokens");

            try
            {
                var cryptoTokens = await _productReadRepository.GetAllProductsAsync<Crypto>(tenantId);
                _logger.LogDebug($"Retrieved all Crypto tokens");

                if (cryptoTokens != null)
                {
                    _logger.LogDebug($"({cryptoTokens.Count()}) crypto tokens retrieved");
                }
                else
                {
                    var errorContent = $"cryptos tokens not found for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                var result = cryptoTokens.Select(st => _productsResponseFactory.MapProduct(tenantId, st)).ToArray();
                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting all crypto tokens. {Environment.NewLine}" +
                    $"Error Message: {ex.Message} {Environment.NewLine}" +
                    $"Stack Trace: {ex.StackTrace}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/products/crypto")]
        public async Task<IActionResult> UpdateCrypto([FromRoute] string tenantId, [FromBody] UpdateCryptoRequest request)
        {
            _logger.LogDebug($"Received request to update Crypto: {JsonConvert.SerializeObject(request)}");

            try
            {
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {request.InstrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                var token = await _productReadRepository.GetProductAsync<Crypto>(tenantId, request.ProductId);
                if (token == null)
                {
                    var errorContent = $"products not found with product id {request.ProductId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                token.Color = request.Color ?? token.Color;
                token.InstrumentId = request.InstrumentId ?? token.InstrumentId;
                token.ExternalAssetId = request.ExternalAssetId ?? token.ExternalAssetId;
                if (!request.IsMinted && token.IsMinted)
                    token.SetAsNotMinted();
                else if (request.IsMinted && !token.IsMinted)
                    token.SetAsMinted();

                await _productWriteRepository.UpdateCryptoTokenAsync(tenantId, token);

                _logger.LogDebug($"Updated crypto token {token.ProductId} for tenant {tenantId}");
                return Ok(await _productsResponseFactory.MapProduct(tenantId, token));

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating Crypto token with Id {request.ProductId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/crypto/{id}")]
        public async Task<IActionResult> GetCrypto([FromRoute] string tenantId, [FromRoute] string id)
        {
            try
            {
                var response = await _productReadRepository.GetProductAsync<Crypto>(tenantId, id);
                if (response == null)
                {
                    var errorContent = $"crypto token not found with product id {id} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                return Ok(response);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting crypto for tenant: {tenantId}, productId: {id}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/crypto")]
        public async Task<IActionResult> CreateCrypto([FromRoute] string tenantId, [FromBody] CreateCryptoRequest request)
        {
            _logger.LogDebug($"Received request to create crypto: {JsonConvert.SerializeObject(request)}");
            var crypto = new Crypto(request.ProductId, request.Color, request.IsMinted)
            {
                ExternalAssetId = string.IsNullOrEmpty(request.ExternalAssetId) ? string.Empty : request.ExternalAssetId,
            };

            try
            {
                //If Instrument exist, Use the existing one else create
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    _logger.LogDebug($"instruments not found.Creating new Instrument with instrument id: {request.InstrumentId} for tenant: {tenantId}.");
                    //Create Instrument .Use the Instrument ID as name to avoid creating with Empty Name..
                    instrument = new Instrument(request.InstrumentId, request.InstrumentId);
                    await _productWriteRepository.AddCryptoProductWithInstrumentAsync(tenantId, crypto, instrument);
                }
                else
                {
                    await _productWriteRepository.AddCryptoProductAsync(tenantId, crypto);
                }
                _logger.LogDebug($"Created crypto {crypto.ProductId} for tenant {tenantId} with instrument {instrument.Id}");

                return Created($"{tenantId}/products/crypto/{crypto.ProductId}", await _productsResponseFactory.MapProduct(tenantId, crypto));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while creating crypto with Id {crypto.ProductId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpDelete("{tenantId}/products/crypto/{productId}")]
        public async Task<IActionResult> DeleteCrypto([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                await _productWriteRepository.RemoveProductAsync(tenantId, productId, ProductTypes.Crypto);
                return Ok();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while removing crypto in tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }
        #endregion
    }
}
