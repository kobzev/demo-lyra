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

    public class SimpleController : ControllerBase
    {
        #region Declaration
        private readonly ILogger<SimpleController> _logger;
        private readonly IProductWriteRepository _productWriteRepository;
        private readonly IProductReadRepository _productReadRepository;
        private readonly ProductsResponseFactory _productsResponseFactory;
        #endregion

        #region Constructor
        public SimpleController(
            ILogger<SimpleController> logger,
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

        [HttpPut("{tenantId}/products/simple")]
        public async Task<IActionResult> UpdateSimple([FromRoute] string tenantId, [FromBody] UpdateSimpleRequest request)
        {
            _logger.LogDebug($"Received request to update Simple token: {JsonConvert.SerializeObject(request)}");

            try
            {
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {request.InstrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                var token = await _productReadRepository.GetProductAsync<Simple>(tenantId, request.ProductId);

                token.Color = request.Color ?? token.Color;
                token.InstrumentId = request.InstrumentId ?? token.InstrumentId;

                await _productWriteRepository.UpdateSimpleTokenAsync(tenantId, token);

                _logger.LogDebug($"Updated simple token {token.ProductId} for tenant {tenantId}");
                return Ok(await _productsResponseFactory.MapProduct(tenantId, token));

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating simple token with Id {request.ProductId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/simple/{id}")]
        public async Task<IActionResult> GetSimple([FromRoute] string tenantId, [FromRoute] string id)
        {
            try
            {
                var response = await _productReadRepository.GetProductAsync<Simple>(tenantId, id);
                if (response == null)
                {
                    var errorContent = $"simple token not found with product id {id} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                return Ok(response);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting simple for tenant: {tenantId}, productId: {id}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/simple")]
        public async Task<IActionResult> CreateSimple([FromRoute] string tenantId, [FromBody] CreateSimpleRequest request)
        {
            _logger.LogDebug($"Received request to create simple: {JsonConvert.SerializeObject(request)}");
            var simple = new Simple(request.ProductId, request.Color);

            try
            {
                //If Instrument exist, Use the existing one else create
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                if (instrument == null)
                {
                    _logger.LogDebug($"instruments not found.Creating new Instrument with instrument id: {request.InstrumentId} for tenant: {tenantId}.");
                    //Create Instrument .Use the Instrument ID as name to avoid creating with Empty Name..
                    instrument = new Instrument(request.InstrumentId, request.InstrumentId);
                    await _productWriteRepository.AddSimpleProductWithInstrumentAsync(tenantId, simple, instrument);
                }
                else
                {
                    await _productWriteRepository.AddSimpleProductAsync(tenantId, simple);
                }
                _logger.LogDebug($"Created simple {simple.ProductId} for tenant {tenantId} with instrument {instrument.Id}");

                return Created($"{tenantId}/products/simple/{simple.ProductId}", await _productsResponseFactory.MapProduct(tenantId, simple));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while creating simple with Id {simple.ProductId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpDelete("{tenantId}/products/simple/{productId}")]
        public async Task<IActionResult> DeleteSimple([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                await _productWriteRepository.RemoveProductAsync(tenantId, productId, ProductTypes.Simple);
                return Ok();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while removing simple in tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }
        #endregion
    }
}
