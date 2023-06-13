namespace Lyra.ManagementApi.Controllers
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Lyra.Api.Models.Products;
    using Lyra.Api.RequestModels.ShareToken;
    using Lyra.ManagementApi.Models;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Lyra.Products;
    using Lyra.Instruments;

    public class ShareTokensController : ControllerBase
    {
        #region Declaration
        private readonly ILogger<ShareTokensController> _logger;
        private readonly IProductWriteRepository _productWriteRepository;
        private readonly IProductReadRepository _productReadRepository;
        private readonly ProductsResponseFactory _productsResponseFactory;

        #endregion

        #region Constructor
        public ShareTokensController(
            ILogger<ShareTokensController> logger,
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

        #region Public Method - API
        // Note (Prashant): Could use GetProducts with a type query.
        [HttpGet("{tenantId}/products/shareTokens")]
        public async Task<IActionResult> GetAllShareTokens(string tenantId)
        {
            _logger.LogDebug($"Received request to get all share tokens");

            try
            {
                var shareTokens = await _productReadRepository.GetAllProductsAsync<ShareToken>(tenantId);
                _logger.LogDebug($"Retrieved all share tokens");

                if (shareTokens != null)
                {
                    _logger.LogDebug($"({shareTokens.Count()}) share tokens retrieved");
                }
                else
                {
                    var errorContent = $"share tokens not found for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                var result = shareTokens.Select(st => _productsResponseFactory.MapShareToken(tenantId, st)).ToArray();
                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting all share tokens. {Environment.NewLine}" +
                    $"Error Message: {ex.Message} {Environment.NewLine}" +
                    $"Stack Trace: {ex.StackTrace}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/products/shareTokens/{tokenId}")]
        public async Task<ActionResult> UpdateShareToken(string tenantId, [FromQuery] string idempotencyToken, [FromBody] UpdateShareTokenRequest request)
        {

            //We should not allow if there is No product Id.
            if (string.IsNullOrWhiteSpace(request.Id))
            {
                var errorContent = $"Product Id is Empty or Null.for tenant {tenantId}";
                _logger.LogWarning(errorContent);
                return BadRequest(errorContent);
            }

            _logger.LogDebug($"Trying to update share token: {JsonConvert.SerializeObject(request)}");
            try
            {
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {request.InstrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                var token = await _productReadRepository.GetProductAsync<ShareToken>(tenantId, request.Id);
                if (token == null)
                {
                    var errorContent = $"products not found with product id {request.Id} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                token.NumberOfDecimalPlaces = request.NumberOfDecimalPlaces ?? token.NumberOfDecimalPlaces;
                token.Color = request.Color ?? token.Color;
                token.Name = request.Name ?? token.Name;
                token.Ticker = request.Ticker ?? token.Ticker;
                token.DocumentUrl = request.DocumentUrl ?? token.DocumentUrl;
                token.InstrumentId = request.InstrumentId ?? token.InstrumentId;
                token.IsDeployed = request.IsDeployed ?? token.IsDeployed;
                token.IsFrozen = request.IsFrozen ?? token.IsFrozen;
                token.BlockchainErrorMessage = request.BlockchainErrorMessage;
                token.TotalSupply = request.TotalSupply ?? token.TotalSupply;
                token.ExternalAssetId = request.ExternalAssetId ?? token.ExternalAssetId;
                if (request.IsMinted.HasValue)
                {
                    if (!request.IsMinted.Value && token.IsMinted)
                        token.SetAsNotMinted();
                    else if (request.IsMinted.Value && !token.IsMinted)
                        token.SetAsMinted();
                }

                await _productWriteRepository.UpdateShareTokenAsync(tenantId, token, idempotencyToken);
                await _productWriteRepository.UpdateInstrumentAsync(tenantId, token.InstrumentId, new Instrument(token.InstrumentId, instrument.Name, instrument.InstrumentStatus, token.NumberOfDecimalPlaces));

                _logger.LogDebug($"Updated share token {token.ProductId} for tenant {tenantId}");
                return Ok(await _productsResponseFactory.MapProduct(tenantId, token));

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating share token with Id {request.Id} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/shareTokens")]
        public async Task<ActionResult> CreateShareToken(string tenantId, [FromBody] CreateShareTokenRequest request)
        {
            //We should not allow if there is No product Id.
            if (string.IsNullOrWhiteSpace(request.ProductId))
            {
                var errorContent = $"Product Id is Empty or Null for tenant {tenantId}";
                _logger.LogDebug(errorContent);
                return BadRequest(errorContent);
            }

            _logger.LogDebug($"Received request to create share token: {JsonConvert.SerializeObject(request)}");

            var shareToken = new ShareToken(request.ProductId, request.Name, request.Ticker,
                request.DocumentUrl, request.IsMinted, request.Color, request.NumberOfDecimalPlaces)
            {
                InstrumentId = request.InstrumentId,
                TotalSupply = request.TotalSupply,
                IsDeployed = request.IsDeployed,
                IsFrozen = request.IsFrozen,
                ExternalAssetId = string.IsNullOrEmpty(request.ExternalAssetId) ? string.Empty : request.ExternalAssetId,
            };
            try
            {

                //If Instrument exist, Use the existing one else create
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    _logger.LogDebug($"instruments not found.Creating new Instrument with instrument id: {request.InstrumentId} for tenant: {tenantId}.");
                    //Create Instrument . Use the Instrument ID as name if empty.
                    instrument = new Instrument((string.IsNullOrEmpty(request.InstrumentId) ? request.ProductId : request.InstrumentId), (string.IsNullOrEmpty(request.Name) ? request.InstrumentId : request.Name))
                    {
                        NumberOfDecimalPlaces = request.NumberOfDecimalPlaces,
                    };
                    await _productWriteRepository.AddShareTokenProductWithInstrumentAsync(tenantId, shareToken, instrument);
                }
                else
                {
                    await _productWriteRepository.AddShareTokenProductAsync(tenantId, shareToken);
                }
                _logger.LogDebug($"Created share token {shareToken.ProductId} for tenant {tenantId} with instrument {instrument.Id}");

                return Created($"{tenantId}/products/{shareToken.ProductId}", await _productsResponseFactory.MapProduct(tenantId, shareToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while creating share token with Id {shareToken.ProductId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/shareTokens/{productId}")]
        public async Task<IActionResult> GetShareToken([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                var shareToken = await _productReadRepository.GetProductAsync<ShareToken>(tenantId, productId);
                if (shareToken != null)
                    return Ok(await _productsResponseFactory.MapProduct(tenantId, shareToken));
                else
                {
                    var errorContent = $"Sharetoken not found for product Id : {productId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return BadRequest(errorContent);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting share token for tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpDelete("{tenantId}/products/shareTokens/{productId}")]
        public async Task<IActionResult> DeleteShareToken([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                await _productWriteRepository.RemoveProductAsync(tenantId, productId, ProductTypes.ShareToken);
                return Ok();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while removing share token in tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }
        #endregion
    }
}
