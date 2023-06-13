namespace Lyra.Api.Controllers
{
    using Lyra.Api.Configuration;
    using Lyra.Api.Models.Products;
    using Lyra.Api.RequestModels.ShareToken;
    using Lyra.Instruments;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using MoreLinq.Extensions;
    using Newtonsoft.Json;
    using shortid;
    using shortid.Configuration;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;
    using System.Threading.Tasks;
    using SubType = Lyra.Products.SubType;

    public class ProductsController : ControllerBase
    {
        private readonly ILogger<ProductsController> _logger;

        private readonly LyraConfiguration _configuration;

        private readonly IProductWriteRepository _productWriteRepository;

        private readonly IProductReadRepository _productReadRepository;


        private const int DefaultMiningByStreamingPercentage = 50;
        private const int DefaultMiningByCurationPercentage = 10;

        public ProductsController(
            ILogger<ProductsController> logger,
            IProductReadRepository productReadRepository,
            IProductWriteRepository productWriteRepository,
            LyraConfiguration configuration)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _productReadRepository = productReadRepository ?? throw new ArgumentNullException(nameof(productReadRepository));
            _productWriteRepository = productWriteRepository ?? throw new ArgumentNullException(nameof(productWriteRepository));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        [Obsolete("Will not take into the account disabled mechanism. Remove after migrating the app.")]
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
                        var mappingTasks = batch.Select(async x => await MapProduct(tenantId, x));
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

                            case ProductTypeModel.CopyrightToken:
                                var copyrightToken = await _productReadRepository.GetAllProductsAsync<CopyrightToken>(tenantId);
                                return (IEnumerable<Product>)copyrightToken;

                            case ProductTypeModel.ShareToken:
                                var shareToken = await _productReadRepository.GetAllProductsAsync<ShareToken>(tenantId);
                                return (IEnumerable<Product>)shareToken;

                            default:
                                return default;
                        }
                    });

                    var filtered = await Task.WhenAll(typesTasks);
                    var mappingTasks = filtered.SelectMany(x => x).Where(x => x != null).Select(async x => await MapProduct(tenantId, x));

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
                        case ProductTypeModel.CopyrightToken:
                            category = ProductTypes.CopyrightToken;
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
                    var mappingTasks = batch.Select(async x => await MapProduct(tenantId, x));
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

        [HttpGet("{tenantId}/enabledproducts/paginated")]
        public async Task<IActionResult> GetEnabledProductsPaginated(
           [FromRoute] string tenantId,
           [FromQuery, Required] int pageSize,
           [FromQuery] string token,
           [FromQuery] string type)
        {
            try
            {
                _logger.LogDebug($"Received request to get all paginated enabled products for tenant {tenantId}.Token: {token}");
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
                        case ProductTypeModel.CopyrightToken:
                            category = ProductTypes.CopyrightToken;
                            break;
                        case ProductTypeModel.ShareToken:
                            category = ProductTypes.ShareToken;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                var all = await _productReadRepository.GetAllEnabledProductsPaginatedAsync(tenantId, category, pageSize, token);

                foreach (var batch in all.Items.Batch(10))
                {
                    var mappingTasks = batch.Select(async x => await MapProduct(tenantId, x));
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
                _logger.LogError(ex, $"Issue while Retreiving Enabled Products with token: {token} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }
        // Note (Rach): Could use GetProducts with a type query.
        [HttpGet("{tenantId}/products/shareTokens")]
        public async Task<IActionResult> GetAllShareTokens(string tenantId)
        {
            _logger.LogDebug($"Received request to get all share tokens");

            try
            {
                var shareTokens = await _productReadRepository.GetAllProductsAsync<ShareToken>(tenantId);

                if (shareTokens != null)
                {
                    _logger.LogDebug($"({shareTokens.Count()}) share tokens retrieved");
                }

                var result = shareTokens.Select(st => MapShareToken(tenantId, st)).ToArray();
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
                _logger.LogDebug($"Product Id is Empty or Null.");
                return BadRequest();
            }

            var serializedRequest = JsonConvert.SerializeObject(request);
            _logger.LogDebug($"Trying to update share token: {serializedRequest}");
            try
            {
                var token = await _productReadRepository.GetProductAsync<ShareToken>(tenantId, request.Id);

                if (token != null)
                {
                    token.NumberOfDecimalPlaces = request.NumberOfDecimalPlaces ?? default;
                    token.Color = request.Color ?? string.Empty;
                    token.Name = request.Name ?? string.Empty;
                    token.Ticker = request.Ticker ?? string.Empty;
                    token.DocumentUrl = request.DocumentUrl ?? string.Empty;
                    token.InstrumentId = request.InstrumentId ?? string.Empty;
                    token.IsDeployed = request.IsDeployed ?? default;
                    token.IsFrozen = request.IsFrozen ?? default;
                    token.BlockchainErrorMessage = request.BlockchainErrorMessage ?? string.Empty;
                    token.TotalSupply = request.TotalSupply ?? default;
                    token.ExternalAssetId = request.ExternalAssetId ?? string.Empty;

                    if (request.IsMinted.HasValue)
                    {
                        if (!request.IsMinted.Value && token.IsMinted)
                            token.SetAsNotMinted();
                        else if (request.IsMinted.Value && !token.IsMinted)
                            token.SetAsMinted();
                    }

                    await _productWriteRepository.UpdateShareTokenAsync(tenantId, token, idempotencyToken);

                    if (!string.IsNullOrEmpty(request.InstrumentId))
                    {
                        var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, request.InstrumentId);

                        if (instrument == null)
                        {
                            _logger.LogDebug($"instruments not found with instrument id {request.InstrumentId} for tenant {tenantId}");
                            return NotFound();
                        }
                        await _productWriteRepository.UpdateInstrumentAsync(tenantId, token.InstrumentId, new Instrument(token.InstrumentId, instrument.Name,
                                                                             instrument.InstrumentStatus, token.NumberOfDecimalPlaces));
                    }

                    _logger.LogDebug($"Updated share token {token.ProductId} for tenant {tenantId}");
                    return Ok(MapShareToken(tenantId, token));
                }
                else
                {
                    _logger.LogDebug($"products not found with product id {request.Id} for tenant {tenantId}");
                    return NotFound();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating share token with Id {request.Id} for tenant {tenantId}. Serailized request body is : {serializedRequest}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/shareTokens")]
        public async Task<ActionResult> CreateShareToken(string tenantId, [FromBody] CreateShareTokenRequest request)
        {
            //We should not allow if there is No product Id.
            if (string.IsNullOrWhiteSpace(request.ProductId))
            {
                _logger.LogDebug($"Product Id is Empty or Null.");
                return BadRequest();
            }

            _logger.LogDebug($"Received request to create share token: {JsonConvert.SerializeObject(request)}");

            var shareToken = new ShareToken(request.ProductId, request.Name, request.Ticker,
               request.DocumentUrl, request.IsMinted, request.Color, request.NumberOfDecimalPlaces)
            {
                InstrumentId = request.InstrumentId,
                TotalSupply = request.TotalSupply,
                IsDeployed = request.IsDeployed,
                IsFrozen = request.IsFrozen,
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

                return Created($"{tenantId}/products/{shareToken.ProductId}", MapShareToken(tenantId, shareToken));
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
                return Ok(MapShareToken(tenantId, shareToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting share token for tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/{productId}")]
        public async Task<IActionResult> GetProduct([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                var product = await _productReadRepository.GetProductAsync(tenantId, productId);
                return Ok(await MapProduct(tenantId, product));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting product for tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/copyrightTokens")]
        public async Task<ActionResult> CreateCopyrightTokenWithInstrument(string tenantId, [FromBody] CreateCopyrightTokenWithInstrumentRequest request)
        {
            _logger.LogDebug($"Received request to create copyright token: {JsonConvert.SerializeObject(request)}");

            try
            {
                var instrumentId = CreateValidInstrumentId(request.CopyrightToken.SubType);

                var instrument = new Instrument(instrumentId,
                    request.Instrument.Name,
                    request.Instrument.NumberOfDecimalPlaces ?? 0);

                var copyrightToken = new CopyrightToken(
                    instrumentId,
                    request.CopyrightToken.ExternalMusicId,
                    request.CopyrightToken.CreatorId,
                    request.CopyrightToken.Icon,
                    request.CopyrightToken.Color,
                    tenantId,
                    request.CopyrightToken.SubType == SubTypeModel.Diamond ? SubType.Diamond : SubType.Golden,
                    request.CopyrightToken.Ownership,
                    request.CopyrightToken.Amount,
                    request.CopyrightToken.TradingVolume,
                    new SongDetails(
                        request.CopyrightToken.SongDetails.Name,
                        request.CopyrightToken.SongDetails.ArtistName,
                        request.CopyrightToken.SongDetails.AlbumName,
                        request.CopyrightToken.SongDetails.Description,
                        request.CopyrightToken.SongDetails.Genre,
                        request.CopyrightToken.SubType == SubTypeModel.Diamond ? DefaultMiningByStreamingPercentage : 0,
                        request.CopyrightToken.SubType == SubTypeModel.Diamond ? DefaultMiningByCurationPercentage : 0)
                    );

                _logger.LogDebug($"Trying to create copyright token: {JsonConvert.SerializeObject(copyrightToken)} with instrument {JsonConvert.SerializeObject(instrument)}");

                await _productWriteRepository.AddCopyrightTokenWithInstrumentAsync(tenantId, copyrightToken, instrument);

                _logger.LogDebug($"Created copyright {copyrightToken.ProductId} for tenant {tenantId} with external music Id: {copyrightToken.ExternalMusicId}");
                return Created($"{tenantId}/products/{copyrightToken.ProductId}", MapCopyrightToken(copyrightToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while creating copyright token with external music Id: {request.CopyrightToken.ExternalMusicId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/products/copyrightTokens/{productId}/contributors")]
        public async Task<IActionResult> UpdateCopyrightTokenContributors(
            [FromRoute] string tenantId,
            [FromRoute] string productId,
            [FromBody] ContributorsModel contributors)
        {
            _logger.LogDebug($"Received request to update copyright token {productId}: {JsonConvert.SerializeObject(contributors)}");

            try
            {
                var copyrightToken = await _productReadRepository.GetProductAsync<CopyrightToken>(tenantId, productId);

                copyrightToken.AddContributors(new Contributors
                {
                    Owners = contributors.Owners?.Select(x => x.ToDomain()).ToList(),
                    Composers = contributors.Composers?.Select(x => x.ToDomain()).ToList(),
                    Lyricists = contributors.Lyricists?.Select(x => x.ToDomain()).ToList(),
                    Songwriters = contributors.Songwriters?.Select(x => x.ToDomain()).ToList(),
                    Producers = contributors.Producers?.Select(x => x.ToDomain()).ToList(),
                    Engineers = contributors.Engineers?.Select(x => x.ToDomain()).ToList(),
                    FeaturedArtists = contributors.FeaturedArtists?.Select(x => x.ToDomain()).ToList(),
                    NonFeaturedMusicians = contributors.NonFeaturedMusicians?.Select(x => x.ToDomain()).ToList(),
                    NonFeaturedVocalists = contributors.NonFeaturedVocalists?.Select(x => x.ToDomain()).ToList(),
                });

                await _productWriteRepository.UpdateCopyrightTokenAsync(tenantId, copyrightToken);

                return Ok(MapCopyrightToken(copyrightToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating copyright token with Id: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/products/copyrightTokens/{productId}/trackingAccount/{trackingAccountId}")]
        public async Task<IActionResult> UpdateCopyrightTokenTrackingAccount(
            [FromRoute] string tenantId,
            [FromRoute] string productId,
            [FromRoute] string trackingAccountId,
            [FromBody] UpdateTrackingAccountModel model)
        {
            _logger.LogDebug($"Received request to update copyright token {productId}: {JsonConvert.SerializeObject(model)}");

            try
            {
                var copyrightToken = await _productReadRepository.GetProductAsync<CopyrightToken>(tenantId, productId);

                copyrightToken.UpdateTrackingAccountIfApplies(trackingAccountId, model.ProfileId);

                await _productWriteRepository.UpdateCopyrightTokenAsync(tenantId, copyrightToken);

                return Ok(MapCopyrightToken(copyrightToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating copyright token with Id: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/copyrightTokens/{productId}/auctioned")]
        public async Task<IActionResult> UpdateCopyrightTokenAuctioned(
            [FromRoute] string tenantId,
            [FromRoute] string productId,
            [FromBody] UpdateCopyrightTokenAuctionedRequest body)
        {
            try
            {
                var copyrightToken = await _productReadRepository.GetProductAsync<CopyrightToken>(tenantId, productId);

                copyrightToken.Auctioned(body.Amount);

                await _productWriteRepository.UpdateCopyrightTokenAsync(tenantId, copyrightToken);

                return Ok(MapCopyrightToken(copyrightToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating copyright token with  Id: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/copyrightTokens/{productId}/minted")]
        [HttpPost("{tenantId}/products/{productId}/minted")]
        public async Task<IActionResult> UpdateProductAsMinted(
            [FromRoute] string tenantId,
            [FromRoute] string productId)
        {
            try
            {
                var product = await _productReadRepository.GetProductAsync<Product>(tenantId, productId);

                product.SetAsMinted();

                await _productWriteRepository.UpdateProductAsMinted(tenantId, product);

                var productModel = new ProductModel()
                {
                    Id = product.ProductId,
                    Category = product.Category,
                    InstrumentId = product.InstrumentId,
                    IsMinted = product.IsMinted,
                    // todo: return number of decimal places
                    // NumberOfDecimalPlaces = product.
                };
                return Ok(productModel);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating copyright token with  Id: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/copyrightTokens/{productId}/miningSettings")]
        public async Task<IActionResult> UpdateCopyrightTokenMiningSettings(
            [FromRoute] string tenantId,
            [FromRoute] string productId,
            [FromBody] UpdateCopyrightTokenMiningSettingsRequest body)
        {
            try
            {
                var copyrightToken = await _productReadRepository.GetProductAsync<CopyrightToken>(tenantId, productId);

                copyrightToken.SetMiningSettings(body.MiningByStreaming, body.MiningByCuration);

                await _productWriteRepository.UpdateCopyrightTokenAsync(tenantId, copyrightToken);

                return Ok(MapCopyrightToken(copyrightToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating copyright token with  Id: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [Obsolete("GetAvailableForAuctionByCreator is deprecated, please use GetByCreator instead.")]
        [HttpGet("{tenantId}/products/copyrightTokens/byCreator/{creatorId}/availableForAuction")]
        public async Task<IActionResult> GetAvailableForAuctionByCreator([FromRoute] string tenantId, [FromRoute] string creatorId)
        {
            try
            {
                var copyrightTokens = await _productReadRepository.GetCopyrightTokensByCreatorIdAsync(tenantId, creatorId);
                var availableForAuction = copyrightTokens.Where(x => x.Amount > x.AlreadyAuctionedAmount);
                var result = availableForAuction.Select(MapCopyrightToken);

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting available copyright tokens by creator for tenant: {tenantId}, creator: {creatorId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/copyrightTokens/byCreator/{creatorId}")]
        public async Task<IActionResult> GetByCreator([FromRoute] string tenantId, [FromRoute] string creatorId)
        {
            try
            {
                var copyrightTokens = await _productReadRepository.GetCopyrightTokensByCreatorIdAsync(tenantId, creatorId);
                var result = copyrightTokens.Select(MapCopyrightToken);

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting copyright tokens by creator for tenant: {tenantId}, creator: {creatorId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/copyrightTokens/{productId}")]
        public async Task<IActionResult> GetCopyrightToken([FromRoute] string tenantId, [FromRoute] string productId)
        {
            try
            {
                var copyrightToken = await _productReadRepository.GetProductAsync<CopyrightToken>(tenantId, productId);

                return Ok(MapCopyrightToken(copyrightToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting copyright token for tenant: {tenantId}, productId: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/copyrightTokens/byExternalMusicId/{externalMusicId}")]
        public async Task<IActionResult> GetCopyrightTokenByExternalMusicId([FromRoute] string tenantId, [FromRoute] string externalMusicId)
        {
            try
            {
                var copyrightTokens = await _productReadRepository.GetCopyrightTokensByExternalMusicIdAsync(tenantId, externalMusicId);
                var result = copyrightTokens.Select(MapCopyrightToken).ToArray();

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting copyright token by external music Id for tenant: {tenantId}, externalMusicId: {externalMusicId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/copyrightTokens/byExternalMusicId")]
        public async Task<IActionResult> GetMultipleCopyrightTokenByExternalMusicId([FromRoute] string tenantId, [FromBody] string[] externalMusicIds)
        {
            try
            {
                var copyrightTokens = await _productReadRepository.GetCopyrightTokensByExternalMusicIdAsync(tenantId, externalMusicIds);

                var result = copyrightTokens.Select(MapCopyrightToken).ToArray();

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting copyright token by external music Ids for tenant: {tenantId}, externalMusicId: {externalMusicIds}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/copyrightTokens/availableForSecondaryMarket")]
        public async Task<IActionResult> GetAvailableForSecondaryMarket([FromRoute] string tenantId)
        {
            try
            {
                var copyrightTokens = await _productReadRepository.GetAvailableForSecondaryMarketAsync(tenantId);

                var result = copyrightTokens.Select(MapCopyrightToken).ToArray();

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting copyright tokens available for secondary market for tenant: {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpGet("{tenantId}/products/copyrightTokens/availableForSecondaryMarketGrouped")]
        public async Task<IActionResult> GetAvailableForSecondaryMarketGrouped([FromRoute] string tenantId)
        {
            try
            {
                var copyrightTokens = await _productReadRepository.GetAvailableForSecondaryMarketAsyncRankByTradingVolume(tenantId);

                var result = copyrightTokens
                    .GroupBy(x => x.ExternalMusicId,
                        token => token,
                        (key, value) => value.Select(y => y))
                    .OrderByDescending(x => x.Sum(t => t.TradingVolume))
                    .SelectMany(x => x)
                    .Select(MapCopyrightToken).ToArray();

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting copyright tokens available for secondary market for tenant: {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        // Note (Rach): Could use GetProducts with a type query.
        [HttpGet("{tenantId}/products/copyrightTokens")]
        public async Task<IActionResult> GetAllCopyrightTokens([FromRoute] string tenantId)
        {
            try
            {
                var copyrightTokens = await _productReadRepository.GetAllProductsAsync<CopyrightToken>(tenantId);
                var result = copyrightTokens.Select(MapCopyrightToken).ToArray();

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while getting copyright tokens for tenant: {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPost("{tenantId}/products/copyrightTokens/{productId}/traded")]
        public async Task<IActionResult> UpdateCopyrightTokenIncreaseTradingVolume(
            [FromRoute] string tenantId,
            [FromRoute] string productId,
            [FromBody] UpdateCopyrightTokenTradedRequest body)
        {
            try
            {
                var copyrightToken = await _productReadRepository.GetProductAsync<CopyrightToken>(tenantId, productId);

                copyrightToken.Traded(body.Amount);

                await _productWriteRepository.UpdateCopyrightTokenAsync(tenantId, copyrightToken);

                return Ok(MapCopyrightToken(copyrightToken));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while updating copyright token with  Id: {productId}");
                return BadRequest(new { ex.Message });
            }
        }

        private CopyrightTokenResponseModel MapCopyrightToken(CopyrightToken copyrightToken)
        {
            return new CopyrightTokenResponseModel
            {
                Id = copyrightToken.ProductId,
                Amount = copyrightToken.Amount,
                AlreadyAuctionedAmount = copyrightToken.AlreadyAuctionedAmount,
                TradingVolume = copyrightToken.TradingVolume,
                Category = copyrightToken.Category,
                CreatorId = copyrightToken.CreatorId,
                ExternalMusicId = copyrightToken.ExternalMusicId,
                Color = copyrightToken.Color,
                Icon = copyrightToken.Icon,
                Ownership = copyrightToken.Ownership,
                IsAvailableForSecondaryMarket = copyrightToken.IsAvailableAtSecondaryMarket,
                IsMinted = copyrightToken.IsMinted,
                SubType = copyrightToken.SubType == SubType.Diamond ? SubTypeModel.Diamond : SubTypeModel.Golden,
                SongDetails = new SongDetailsModel
                {
                    Name = copyrightToken.SongDetails.Name,
                    AlbumName = copyrightToken.SongDetails.AlbumName,
                    ArtistName = copyrightToken.SongDetails.ArtistName,
                    Description = copyrightToken.SongDetails.Description,
                    Genre = copyrightToken.SongDetails.Genre,
                    MiningByCuration = copyrightToken.SongDetails.MiningByCuration,
                    MiningByStreaming = copyrightToken.SongDetails.MiningByStreaming,
                    Contributors = new ContributorsModel
                    {
                        Owners = copyrightToken.SongDetails.Contributors.Owners.Select(x => new ContributorModel(x)).ToList(),
                        Composers = copyrightToken.SongDetails.Contributors.Composers.Select(x => new ContributorModel(x)).ToList(),
                        FeaturedArtists = copyrightToken.SongDetails.Contributors.FeaturedArtists.Select(x => new ContributorModel(x)).ToList(),
                        NonFeaturedMusicians = copyrightToken.SongDetails.Contributors.NonFeaturedMusicians.Select(x => new ContributorModel(x)).ToList(),
                        NonFeaturedVocalists = copyrightToken.SongDetails.Contributors.NonFeaturedVocalists.Select(x => new ContributorModel(x)).ToList(),
                        Songwriters = copyrightToken.SongDetails.Contributors.Songwriters.Select(x => new ContributorModel(x)).ToList(),
                        Producers = copyrightToken.SongDetails.Contributors.Producers.Select(x => new ContributorModel(x)).ToList(),
                        Engineers = copyrightToken.SongDetails.Contributors.Engineers.Select(x => new ContributorModel(x)).ToList(),
                        Lyricists = copyrightToken.SongDetails.Contributors.Lyricists.Select(x => new ContributorModel(x)).ToList(),
                    }
                },
                InstrumentId = copyrightToken.InstrumentId
            };
        }

        private ShareTokenResponseModel MapShareToken(string tenantId, ShareToken shareToken)
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

        private async Task<ProductModel> MapProduct(string tenantId, Product product)
        {
            try
            {
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, product.InstrumentId);

                if (instrument == null)
                {
                    _logger.LogDebug($"instruments not found with instrument id {product.InstrumentId} for tenant {tenantId}");
                }

                if (product is CopyrightToken copyrightToken)
                {
                    return new CopyrightTokenResponseOldModel
                    {
                        Id = copyrightToken.ProductId,
                        Name = instrument?.Name ?? copyrightToken.ProductId,
                        AlbumName = copyrightToken.SongDetails?.AlbumName,
                        ArtistName = copyrightToken.SongDetails?.ArtistName,
                        Card = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-card.png"),
                        Code = instrument?.Symbol ?? copyrightToken.ProductId,
                        Category = copyrightToken.Category,
                        Color = copyrightToken.Color,
                        Cover = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-cover.png"),
                        ExternalMusicId = copyrightToken.ExternalMusicId,
                        Disabled = false,
                        Description = copyrightToken.SongDetails?.Description,
                        Amount = copyrightToken.Amount,
                        AlreadyAuctionedAmount = copyrightToken.AlreadyAuctionedAmount,
                        UnitsAvailable = copyrightToken.Amount - copyrightToken.AlreadyAuctionedAmount,
                        Icon = "MyLyCI",
                        InstrumentId = product.InstrumentId,
                        ProductStatus = copyrightToken.ProductStatus
                    };
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
                        Card = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-card.png"),
                        Cover = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-cover.png"),
                        Color = crypto.Color,
                        InstrumentId = product.InstrumentId,
                        ProductStatus = product.ProductStatus,
                        IsMinted = product.IsMinted,
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
                        Card = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-card.png"),
                        Cover = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-cover.png"),
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
                        Card = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-card.png"),
                        Cover = CloudFrontUrl(_configuration.CloudFrontUrl, $"{instrument?.Url}/{instrument?.Url}-cover.png"),
                        Color = fiat.Color,
                        InstrumentId = product.InstrumentId,
                        ProductStatus = product.ProductStatus,
                    };
                }
                else if (product is ShareToken shareToken)
                {
                    return MapShareToken(tenantId, shareToken);
                }

                return new ProductModel
                {
                    Id = product.ProductId,
                    Category = product.Category,
                    InstrumentId = product.InstrumentId,
                    ProductStatus = product.ProductStatus,
                    IsMinted = product.IsMinted
                };
            }
            catch (Exception ex)
            {
                var dynamoAttributes = JsonConvert.SerializeObject(product);
                _logger.LogError("Exception ocurred and the message is  {Exception} and the serialized message is {Attribute}", ex, dynamoAttributes);
                throw;
            }
        }

        private static string CloudFrontUrl(string cloudFrontUrl, string end)
        {
            return cloudFrontUrl + "misc/products/" + end;
        }

        private static string CreateValidInstrumentId(SubTypeModel subtype)
        {
            var options = new GenerationOptions
            {
                UseNumbers = false,
                UseSpecialCharacters = false,
                Length = 8
            };

            return $"token.{subtype.ToString().ToLower()}.{ShortId.Generate(options).ToUpper()}";
        }
    }
}
