namespace Lyra.ManagementApi.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;
    using System.Threading.Tasks;
    using Lyra.Api;
    using Lyra.Api.Models.Products;
    using Lyra.ManagementApi.Models;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Authorization;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using MoreLinq.Extensions;
    using Newtonsoft.Json;

    [Authorize(Infrastructure.Constants.SecurityPolicy)]
    public class ProductsController : ControllerBase
    {
        #region Declaration
        private readonly ILogger<ProductsController> _logger;
        private readonly IProductReadRepository _productReadRepository;
        private readonly IProductWriteRepository _productWriteRepository;
        private readonly ProductsResponseFactory _productsResponseFactory;

        #endregion

        #region Constructor
        public ProductsController(
            ILogger<ProductsController> logger,
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

        #region public methond API
        [HttpGet("{tenantId}/products")]
        public async Task<IActionResult> GetProducts(string tenantId, [FromQuery] string type)
        {
            try
            {
                _logger.LogDebug($"Received request to get all products for tenant {tenantId}. Type: {type}");
                List<ProductModel> products = default;

                if (string.IsNullOrWhiteSpace(type))
                {
                    products = new List<ProductModel>();
                    var all = await _productReadRepository.GetAllProductsAsync(tenantId);

                    foreach (var batch in all.Batch(10))
                    {
                        var mappingTasks = batch.Select(async x => await _productsResponseFactory.MapProduct(tenantId, x));
                        await Task.WhenAll(mappingTasks);
                        products.AddRange(mappingTasks.Select(t => t.Result));
                    }
                }
                else
                {
                    var types = Helpers.FilterFromUserInput(type);
                    var typesTasks = types.Select(async t =>
                    {
                        switch (t)
                        {
                            case ProductTypeModel.Crypto:
                                var crypto = await _productReadRepository.GetAllProductsAsync<Crypto>(tenantId);
                                return (IEnumerable<Product>)crypto;

                            case ProductTypeModel.Simple:
                                var simple = await _productReadRepository.GetAllProductsAsync<Simple>(tenantId);
                                return (IEnumerable<Product>)simple;

                            case ProductTypeModel.Fiat:
                                var fiat = await _productReadRepository.GetAllProductsAsync<Fiat>(tenantId);
                                return (IEnumerable<Product>)fiat;

                            case ProductTypeModel.ShareToken:
                                var shareToken = await _productReadRepository.GetAllProductsAsync<ShareToken>(tenantId);
                                return (IEnumerable<Product>)shareToken;

                            default:
                                return default;
                        }
                    });

                    var filtered = await Task.WhenAll(typesTasks);
                    var mappingTasks = filtered.SelectMany(x => x).Where(x => x != null).Select(async x => await _productsResponseFactory.MapProduct(tenantId, x));

                    products = (await Task.WhenAll(mappingTasks)).ToList();
                }

                _logger.LogDebug($"Returning {products.Count} products for Tenant {tenantId}");
                return Ok(products);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while Retreiving get all products for tenant {tenantId}. Type: {type}");
                return BadRequest(new
                {
                    ex.Message
                });
            }
        }

        [HttpGet("{tenantId}/products/paginated")]
        public async Task<IActionResult> GetProductsPaginated(
            [FromRoute] string tenantId,
            [FromQuery, Required] int pageSize,
            [FromQuery] string token,
            [FromQuery] string type)
        {
            try
            {
                _logger.LogDebug($"Received request to get all paginated products for tenant {tenantId}. Type: {type}. Token: {token}");
                var products = new List<ProductModel>();

                string category = null;

                if (!string.IsNullOrWhiteSpace(type))
                {
                    Enum.TryParse<ProductTypeModel>(type, true, out var typeParsed);

                    switch (typeParsed)
                    {
                        case ProductTypeModel.Unknown:
                            break;
                        case ProductTypeModel.Crypto:
                            category = ProductTypes.Crypto;
                            break;
                        case ProductTypeModel.Simple:
                            category = ProductTypes.Simple;
                            break;
                        case ProductTypeModel.Fiat:
                            category = ProductTypes.Fiat;
                            break;
                        case ProductTypeModel.ShareToken:
                            category = ProductTypes.ShareToken;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                var all = await _productReadRepository.GetAllProductsPaginatedAsync(tenantId, category, pageSize, token);

                foreach (var batch in all.Items.Batch(10))
                {
                    var mappingTasks = batch.Select(async x => await _productsResponseFactory.MapProduct(tenantId, x));
                    products.AddRange(await Task.WhenAll(mappingTasks));
                }

                var result = new ListResult<ProductModel>()
                {
                    HasNext = all.HasNext,
                    HasPrevious = all.HasPrevious,
                    Next = all.Next,
                    Previous = all.Previous,
                    PageSize = all.PageSize,
                    Items = products.ToList()
                };

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while Retreiving get all products for tenant {tenantId}. Type: {type}");
                return BadRequest(new
                {
                    ex.Message
                });
            }
        }

        [HttpGet("{tenantId}/products/{productId}")]
        public async Task<IActionResult> GetProduct([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                _logger.LogDebug($"Received request to get product by id: {productId} for tenant {tenantId}.");
                var product = await _productReadRepository.GetProductAsync(tenantId, productId);
                return Ok(await _productsResponseFactory.MapProduct(tenantId, product));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting product for tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/products/{productId}/disable")]
        public async Task<IActionResult> DisableProduct([FromRoute] string tenantId,
            [FromRoute] string productId)
        {
            try
            {
                _logger.LogDebug($"Received request to disable product Status with Id {productId} for tenant {tenantId}");
                var status = (int)ProductsStatus.Disabled;
                var product = await _productReadRepository.GetProductAsync(tenantId, productId);

                await _productWriteRepository.UpdateProductStatusAsync(tenantId, product, status);
                _logger.LogDebug($"Updated Product Status {product.ProductId} for tenant {tenantId}");
                return Ok();

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while disabling the Product Status with Id {productId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }


        [HttpPut("{tenantId}/products/{productId}/enable")]
        public async Task<IActionResult> EnableProduct([FromRoute] string tenantId,
            [FromRoute] string productId)
        {
            try
            {
                _logger.LogDebug($"Received request to enable product Status with Id {productId} for tenant {tenantId}");

                var status = (int)ProductsStatus.Enabled;
                var product = await _productReadRepository.GetProductAsync(tenantId, productId);

                await _productWriteRepository.UpdateProductStatusAsync(tenantId, product, status);
                _logger.LogDebug($"Updated Product Status {product.ProductId} for tenant {tenantId}");
                return Ok();

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while enabling the Product Status with Id {productId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        #endregion
    }
}
