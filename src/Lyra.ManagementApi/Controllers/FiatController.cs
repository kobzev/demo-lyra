namespace Lyra.ManagementApi.Controllers
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Lyra.Instruments;
    using Lyra.ManagementApi.Models;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public class FiatController : ControllerBase
    {
        #region Declaration
        private readonly ILogger<FiatController> _logger;
        private readonly IProductWriteRepository _productWriteRepository;
        private readonly IProductReadRepository _productReadRepository;
        private readonly ProductsResponseFactory _productsResponseFactory;
        #endregion

        #region Constructor
        public FiatController(
            ILogger<FiatController> logger,
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

        [HttpGet("{tenantId}/products/fiat/{id}")]
        public async Task<IActionResult> GetFiat([FromRoute] string tenantId, [FromRoute] string id)
        {
            try
            {
                var response = await _productReadRepository.GetProductAsync<Fiat>(tenantId, id);
                if (response == null)
                {
                    var errorContent = $"fiat token not found with product id {id} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                return Ok(response);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting fiat for tenant: {tenantId}, productId: {id}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/products/fiat")]
        public async Task<IActionResult> UpdateFiat([FromRoute] string tenantId, [FromBody] UpdateFiatRequest request)
        {
            _logger.LogDebug($"Received request to update fiat token: {JsonConvert.SerializeObject(request)}");

            try
            {
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {request.InstrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                var token = await _productReadRepository.GetProductAsync<Fiat>(tenantId, request.ProductId);

                token.Color = request.Color ?? token.Color;
                token.InstrumentId = request.InstrumentId ?? token.InstrumentId;

                await _productWriteRepository.UpdateFiatTokenAsync(tenantId, token);

                _logger.LogDebug($"Updated fiat token {token.ProductId} for tenant {tenantId}");
                return Ok(await _productsResponseFactory.MapProduct(tenantId, token));

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating fiat token with Id {request.ProductId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/fiat")]
        public async Task<IActionResult> CreateFiat([FromRoute] string tenantId, [FromBody] CreateFiatRequest request)
        {
            _logger.LogDebug($"Received request to create fiat: {JsonConvert.SerializeObject(request)}");
            var fiat = new Fiat(request.ProductId, request.Color);

            try
            {
                //If Instrument exist, Use the existing one else create
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    _logger.LogDebug($"instruments not found.Creating new Instrument with instrument id: {request.InstrumentId} for tenant: {tenantId}.");
                    //Create Instrument .Use the Instrument ID as name to avoid creating with Empty Name..
                    instrument = new Instrument(request.InstrumentId, request.InstrumentId);
                    await _productWriteRepository.AddFiatProductWithInstrumentAsync(tenantId, fiat, instrument);
                }
                else
                {
                    await _productWriteRepository.AddFiatProductAsync(tenantId, fiat);
                }
                _logger.LogDebug($"Created fiat {fiat.ProductId} for tenant {tenantId} with instrument {instrument.Id}");

                return Created($"{tenantId}/products/fiat/{fiat.ProductId}", await _productsResponseFactory.MapProduct(tenantId, fiat));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while creating fiat with Id {fiat.ProductId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpDelete("{tenantId}/products/fiat/{productId}")]
        public async Task<IActionResult> DeleteFiat([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                await _productWriteRepository.RemoveProductAsync(tenantId, productId, ProductTypes.Fiat);
                return Ok();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while removing fiat in tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }
        #endregion
    }
}
