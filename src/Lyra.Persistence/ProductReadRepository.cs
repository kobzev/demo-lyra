namespace Lyra.Persistence
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.DynamoDBv2.Model;
    using LykkeCorp.DynamoDBv2.Pagination;
    using Lyra.Instruments;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public sealed class ProductReadRepository : IProductReadRepository
    {
        private readonly IDynamoDBContext context;

        private readonly IAmazonDynamoDB _client;
        private readonly ILogger _logger;


        public ProductReadRepository(IAmazonDynamoDB client, ILogger<ProductReadRepository> logger)
        {
            _client = client;
            context = SharedContext.Instance.GetSharedDynamoDBContext(_client) ??
                throw new ArgumentNullException(nameof(_client));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<IEnumerable<Product>> GetAllProductsAsync(string tenant)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;
            do
            {
                var response = await _client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = Table.Products,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            {
                                ":partitionKeyPrefix",
                                new AttributeValue($"TENANT#{tenant}#{Schema.Product.PartitionKeyPrefix}")
                            },
                        },
                        FilterExpression = "begins_with(#partitionKey, :partitionKeyPrefix)"
                    });
                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            if (result == null || result.Count == 0)
            {
                return new Product[0];
            }
            var products = result.Select(MapProduct);
            return products;
        }

        public async Task<ListResult<Product>> GetAllProductsPaginatedAsync(string tenant, string category,
            int pageSize, string paginationToken)
        {
            var token = string.IsNullOrEmpty(paginationToken)
                ? new PaginationTokenWrapper(true)
                : new PaginationTokenWrapper(paginationToken);
            var direction = new PaginationDirection(token.ReadForwards, token.InnerToken);

            var responses = new QueryResponse();
            do
            {
                var request = new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.Products,
                    Limit = pageSize,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#partitionKey", Schema.Product.CommonAttributes.ProductPartitionKey },
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { ":partitionKey", new AttributeValue(Schema.Product.GetProductPartitionKey(tenant)) }
                    },
                    KeyConditionExpression = "#partitionKey = :partitionKey"
                };

                if (!string.IsNullOrWhiteSpace(category))
                {
                    request.ExpressionAttributeNames["#sortKey"] = Schema.Product.CommonAttributes.ProductSortKey;
                    request.ExpressionAttributeValues[":sortKeyPrefix"] = new AttributeValue(category.ToUpper());
                    request.KeyConditionExpression =
                        $"{request.KeyConditionExpression} and begins_with(#sortKey, :sortKeyPrefix)";
                }

                if (responses.LastEvaluatedKey.Count > 0)
                {
                    request.ExclusiveStartKey = responses.LastEvaluatedKey;
                }
                else if (!string.IsNullOrWhiteSpace(paginationToken))
                {
                    request.ExclusiveStartKey = direction.Token.ToAttributeMap();
                }

                var response = await _client.QueryAsync(request);
                var toCopy =
                    response.Items.GetRange(0, Math.Min(response.Items.Count, pageSize - responses.Items.Count));
                responses.Items.AddRange(toCopy);
                responses.Count += toCopy.Count;
                responses.LastEvaluatedKey = response.LastEvaluatedKey;
            } while (responses.LastEvaluatedKey != null && responses.LastEvaluatedKey.Count > 0 &&
                     responses.Count < pageSize);

            var result = new PaginatedList<Product>(
                responses,
                direction,
                (attributeMap) => new PaginationToken(
                    attributeMap,
                    Schema.Attributes.PartitionKey,
                    Schema.Attributes.SortKey,
                    Schema.Product.CommonAttributes.ProductPartitionKey,
                    Schema.Product.CommonAttributes.ProductSortKey),
                MapProduct);

            return new ListResult<Product>
            {
                Items = result.ToList(),
                HasPrevious = result.CanMoveForwards,
                Previous = result.CanMoveForwards ? new PaginationTokenWrapper(true, result.MoveForwards.Token) : null,
                HasNext = result.CanMoveBackwards,
                Next = result.CanMoveBackwards ? new PaginationTokenWrapper(false, result.MoveBackwards.Token) : null,
                PageSize = result.Count,
            };
        }

        public async Task<ListResult<Product>> GetAllEnabledProductsPaginatedAsync(string tenant, string category,
                                                                                   int pageSize, string paginationToken)
        {

            var token = string.IsNullOrEmpty(paginationToken)
                ? new PaginationTokenWrapper(true)
                : new PaginationTokenWrapper(paginationToken);
            var direction = new PaginationDirection(token.ReadForwards, token.InnerToken);
            var responses = new QueryResponse();

            try
            {
                do
                {
                    var request = new QueryRequest
                    {
                        TableName = Table.Products,
                        IndexName = Table.Indicies.ProductStatus,
                        Limit = pageSize,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#partitionKey", Schema.Product.CommonAttributes.ProductStatusPartitionKey }
                    },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { ":partitionKey", new AttributeValue(Schema.Product.GetProductStatusPartitionKey(tenant,(int)ProductsStatus.Enabled)) }
                    },
                        KeyConditionExpression = "#partitionKey = :partitionKey"
                    };

                    if (!string.IsNullOrWhiteSpace(category))
                    {
                        request.ExpressionAttributeNames["#sortKey"] = Schema.Product.CommonAttributes.ProductStatusSortKey;
                        request.ExpressionAttributeValues[":sortKeyPrefix"] = new AttributeValue(category.Trim().ToUpper());
                        request.KeyConditionExpression =
                        $"{request.KeyConditionExpression} and begins_with(#sortKey, :sortKeyPrefix)";
                    }

                    if (responses.LastEvaluatedKey.Count > 0)
                    {
                        request.ExclusiveStartKey = responses.LastEvaluatedKey;
                    }
                    else if (!string.IsNullOrWhiteSpace(paginationToken))
                    {
                        request.ExclusiveStartKey = direction.Token.ToAttributeMap();
                    }

                    var response = await _client.QueryAsync(request);

                    var toCopy =
                        response.Items.GetRange(0, Math.Min(response.Items.Count, pageSize - responses.Items.Count));
                    responses.Items.AddRange(toCopy);
                    responses.Count += toCopy.Count;
                    responses.LastEvaluatedKey = response.LastEvaluatedKey;
                } while (responses.LastEvaluatedKey != null && responses.LastEvaluatedKey.Count > 0 &&
                         responses.Count < pageSize);

                var result = new PaginatedList<Product>(
                    responses,
                    direction,
                    (attributeMap) => new PaginationToken(
                        attributeMap,
                        Schema.Attributes.PartitionKey,
                        Schema.Attributes.SortKey,
                        Schema.Product.CommonAttributes.ProductStatusPartitionKey,
                        Schema.Product.CommonAttributes.ProductStatusSortKey),
                    MapProduct);

                return new ListResult<Product>
                {
                    Items = result.Where(item => item != null).ToList(), // Filter due to Nullable Return.There is a chance of Corrupted Data.
                    HasPrevious = result.CanMoveForwards,
                    Previous = result.CanMoveForwards ? new PaginationTokenWrapper(true, result.MoveForwards.Token) : null,
                    HasNext = result.CanMoveBackwards,
                    Next = result.CanMoveBackwards ? new PaginationTokenWrapper(false, result.MoveBackwards.Token) : null,
                    PageSize = result.Count,
                };
            }
            catch (Exception ex)
            {
                var dynamoAttributes = JsonConvert.SerializeObject(responses);
                _logger.LogError("Exception ocurred while retriving enabled paginated products:  {Exception} and the serialized message is {Attribute}", ex, dynamoAttributes);
                throw;
            }
        }

        public async Task<IEnumerable<T>> GetAllProductsAsync<T>(string tenant) where T : Product
        {
            var category = GetProductCategory(typeof(T));

            var query = await _client.QueryAsync(
                new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.Products,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#partitionKey", Schema.Product.CommonAttributes.ProductPartitionKey },
                        { "#sortKey", Schema.Product.CommonAttributes.ProductSortKey },
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { ":partitionKey", new AttributeValue(Schema.Product.GetProductPartitionKey(tenant)) },
                        { ":sortKeyPrefix", new AttributeValue(category.ToUpper()) }
                    },
                    KeyConditionExpression = "#partitionKey = :partitionKey and begins_with(#sortKey, :sortKeyPrefix)"
                });

            if (query == null || query.Count == 0)
            {
                return new T[0];
            }

            var products = query.Items.Select(item => (T)MapProduct(item));
            return products;
        }

        public async Task<Product> GetProductAsync(string tenant, string productId)
        {
            var query = await _client.QueryAsync(
                new QueryRequest
                {
                    TableName = Table.Products,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#key", Schema.Attributes.PartitionKey }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { ":key", new AttributeValue(Schema.Product.GetPrimaryPartitionKey(tenant, productId)) }
                    },
                    KeyConditionExpression = "#key = :key"
                });

            if (query == null || query.Count == 0)
            {
                return default;
            }

            return query.Items.Select(MapProduct).Single();
        }

        public async Task<T> GetProductAsync<T>(string tenant, string productId) where T : Product
        {
            var product = await GetProductAsync(tenant, productId);

            if (product != null)
            {
                return (T)product;
            }

            return default;
        }

        public async Task<IEnumerable<Instrument>> GetAllInstrumentsAsync(string tenant)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;
            do
            {
                var response = await _client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = Table.Products,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            {
                                ":partitionKeyPrefix",
                                new AttributeValue($"TENANT#{tenant}#{Schema.Instrument.PartitionKeyPrefix}")
                            },
                        },
                        FilterExpression = "begins_with(#partitionKey, :partitionKeyPrefix)"
                    });

                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            if (result.Count == 0)
            {
                return new Instrument[0];
            }

            var instruments = result.Select(MapInstrument);
            return instruments;
        }

        public async Task<ListResult<Instrument>> GetAllInstrumentsPaginatedAsync(string tenant, int pageSize, string paginationToken)
        {
            var token = string.IsNullOrEmpty(paginationToken)
             ? new PaginationTokenWrapper(true)
             : new PaginationTokenWrapper(paginationToken);

            var direction = new PaginationDirection(token.ReadForwards, token.InnerToken);

            var responses = new QueryResponse();
            do
            {
                var request = new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.Instruments,
                    Limit = pageSize,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#partitionKey", Schema.Instrument.Attributes.InstrumentTenantPartitionKey },
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { ":partitionKey", new AttributeValue(Schema.Instrument.GetTenantPartitionKey(tenant)) }
                    },
                    KeyConditionExpression = "#partitionKey = :partitionKey"
                };

                if (responses.LastEvaluatedKey.Count > 0)
                {
                    request.ExclusiveStartKey = responses.LastEvaluatedKey;
                }
                else if (!string.IsNullOrWhiteSpace(paginationToken))
                {
                    request.ExclusiveStartKey = direction.Token.ToAttributeMap();
                }

                var response = await _client.QueryAsync(request);
                var toCopy =
                    response.Items.GetRange(0, Math.Min(response.Items.Count, pageSize - responses.Items.Count));
                responses.Items.AddRange(toCopy);
                responses.Count += toCopy.Count;
                responses.LastEvaluatedKey = response.LastEvaluatedKey;
            } while (responses.LastEvaluatedKey != null && responses.LastEvaluatedKey.Count > 0 &&
                     responses.Count < pageSize);

            var result = new PaginatedList<Instrument>(
                responses,
                direction,
                (attributeMap) => new PaginationToken(
                    attributeMap,
                    Schema.Attributes.PartitionKey,
                    Schema.Attributes.SortKey,
                    Schema.Instrument.Attributes.InstrumentTenantPartitionKey,
                    Schema.Instrument.Attributes.InstrumentTenantSortKey),
                MapInstrument);
            return new ListResult<Instrument>
            {
                Items = result.ToList(),
                HasPrevious = result.CanMoveForwards,
                Previous = result.CanMoveForwards ? new PaginationTokenWrapper(true, result.MoveForwards.Token) : null,
                HasNext = result.CanMoveBackwards,
                Next = result.CanMoveBackwards ? new PaginationTokenWrapper(false, result.MoveBackwards.Token) : null,
                PageSize = result.Count,
            };
        }

        public async Task<ListResult<Instrument>> GetAllEnabledInstrumentsPaginatedAsync(string tenant, int pageSize, string paginationToken)
        {
            var token = string.IsNullOrEmpty(paginationToken)
             ? new PaginationTokenWrapper(true)
             : new PaginationTokenWrapper(paginationToken);

            var direction = new PaginationDirection(token.ReadForwards, token.InnerToken);

            var responses = new QueryResponse();
            do
            {
                var request = new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.InstrumentStatus,
                    Limit = pageSize,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#partitionKey", Schema.Instrument.Attributes.InstrumentStatusPartitionKey }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                       { ":partitionKey", new AttributeValue(Schema.Instrument.GetInstrumentStatusPartitionKey(tenant,(int)InstrumentStatus.Enabled)) }
                    },
                    KeyConditionExpression = "#partitionKey = :partitionKey"
                };

                if (responses.LastEvaluatedKey.Count > 0)
                {
                    request.ExclusiveStartKey = responses.LastEvaluatedKey;
                }
                else if (!string.IsNullOrWhiteSpace(paginationToken))
                {
                    request.ExclusiveStartKey = direction.Token.ToAttributeMap();
                }

                var response = await _client.QueryAsync(request);
                var toCopy =
                    response.Items.GetRange(0, Math.Min(response.Items.Count, pageSize - responses.Items.Count));
                responses.Items.AddRange(toCopy);
                responses.Count += toCopy.Count;
                responses.LastEvaluatedKey = response.LastEvaluatedKey;
            } while (responses.LastEvaluatedKey != null && responses.LastEvaluatedKey.Count > 0 &&
                     responses.Count < pageSize);

            var result = new PaginatedList<Instrument>(
                responses,
                direction,
                (attributeMap) => new PaginationToken(
                    attributeMap,
                    Schema.Attributes.PartitionKey,
                    Schema.Attributes.SortKey,
                    Schema.Instrument.Attributes.InstrumentStatusPartitionKey,
                    Schema.Instrument.Attributes.InstrumentStatusSortKey),
                MapInstrument);
            return new ListResult<Instrument>
            {
                Items = result.ToList(),
                HasPrevious = result.CanMoveForwards,
                Previous = result.CanMoveForwards ? new PaginationTokenWrapper(true, result.MoveForwards.Token) : null,
                HasNext = result.CanMoveBackwards,
                Next = result.CanMoveBackwards ? new PaginationTokenWrapper(false, result.MoveBackwards.Token) : null,
                PageSize = result.Count,
            };
        }


        private static Instrument MapInstrument(Dictionary<string, AttributeValue> item)
        {
            _ = item.TryGetValue(Schema.Instrument.Attributes.InstrumentStatus, out var instrumentStatusAttribute);
            _ = item.TryGetValue(Schema.Instrument.Attributes.NumberOfDecimalPlaces, out var numberOfDecimalPlacesAttrVal);

            return new Instrument(item[Schema.Instrument.Attributes.Id].S,
                item[Schema.Instrument.Attributes.Name].S,
                int.TryParse(instrumentStatusAttribute?.N, out var InstrumentStatus) ? InstrumentStatus : 0,
                int.TryParse(numberOfDecimalPlacesAttrVal?.N, out var numberOfDecimalPlaces)
                    ? numberOfDecimalPlaces : 0);
        }

        public async Task<Instrument> GetInstrumentAsync(string tenant, string instrumentId)
        {
            if (string.IsNullOrEmpty(instrumentId))
            {
                _logger.LogDebug("Instrument not found for tenant {TenantId}", tenant);
                return default;
            }

            var partitionKey = Schema.Instrument.GetPartitionKey(tenant, instrumentId);
            var sortKey = Schema.Instrument.GetSortKey();

            var item = await _client.GetItemAsync(
                new GetItemRequest
                {
                    TableName = Table.Products,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { Schema.Attributes.PartitionKey, new AttributeValue(partitionKey) },
                        { Schema.Attributes.SortKey, new AttributeValue(sortKey) },
                }
                });

            if (item == null || !item.IsItemSet)
            {
                _logger.LogDebug("Instrument not found with id : {InstrumentId} for tenant {TenantId}", instrumentId, tenant);
                return default;
            }

            var document = Document.FromAttributeMap(item.Item);
            var instrumentDto = context.FromDocument<Schema.Instrument>(document);
            return new Instrument(instrumentDto.Id, instrumentDto.Name,
                int.TryParse(instrumentDto.NumberOfDecimalPlaces, out var numberOfDecimalPlaces) ? numberOfDecimalPlaces : 0)
            {
                InstrumentStatus = instrumentDto.InstrumentStatus,
                NumberOfDecimalPlaces = numberOfDecimalPlaces
            };
        }

        public async Task<IEnumerable<CopyrightToken>> GetCopyrightTokensByCreatorIdAsync(string tenant,
            string creatorId)
        {
            var query = await _client.QueryAsync(
                new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.CopyrightTokensByCreatorId,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#creator", Schema.CopyrightToken.Attributes.CreatorIdPartitionKey }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        {
                            ":creator",
                            new AttributeValue(Schema.CopyrightToken.GetCreatorIdPartitionKey(tenant, creatorId))
                        }
                    },
                    KeyConditionExpression = "#creator = :creator"
                });

            if (query == null || query.Count == 0)
            {
                return Array.Empty<CopyrightToken>();
            }

            return query.Items.Select(x => MapProduct(x) as CopyrightToken);
        }

        public async Task<IEnumerable<CopyrightToken>> GetCopyrightTokensByExternalMusicIdAsync(string tenant,
            string externalMusicId)
        {
            var query = await _client.QueryAsync(
                new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.CopyrightTokensByExternalMusicId,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#external_music_id", Schema.CopyrightToken.Attributes.ExternalMusicIdPartitionKey }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        {
                            ":external_music_id",
                            new AttributeValue(
                                Schema.CopyrightToken.GetExternalMusicIdPartitionKey(tenant, externalMusicId))
                        }
                    },
                    KeyConditionExpression = "#external_music_id = :external_music_id"
                });

            if (query == null || query.Count == 0)
            {
                return Array.Empty<CopyrightToken>();
            }

            return query.Items.Select(x => MapProduct(x) as CopyrightToken);
        }

        public async Task<IEnumerable<CopyrightToken>> GetCopyrightTokensByExternalMusicIdAsync(string tenant,
            IEnumerable<string> externalMusicIds)
        {
            var result = new List<CopyrightToken>();

            foreach (var externalMusicId in externalMusicIds)
            {
                result.AddRange(await GetCopyrightTokensByExternalMusicIdAsync(tenant, externalMusicId));
            }

            return result;
        }

        public async Task<IEnumerable<CopyrightToken>> GetAvailableForSecondaryMarketAsync(string tenant)
        {
            var query = await _client.QueryAsync(
                new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.CopyrightTokensBySecondaryMarketAvailability,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        {
                            "#secondary_market_availability",
                            Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey
                        }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        {
                            ":secondary_market_availability",
                            new AttributeValue(
                                Schema.CopyrightToken.GetSecondaryMarketAvailabilityPartitionKey(tenant, true))
                        }
                    },
                    KeyConditionExpression = "#secondary_market_availability = :secondary_market_availability"
                });

            if (query == null || query.Count == 0)
            {
                return Array.Empty<CopyrightToken>();
            }

            return query.Items.Select(x => MapProduct(x) as CopyrightToken);
        }

        public async Task<IEnumerable<CopyrightToken>> GetAvailableForSecondaryMarketAsyncRankByTradingVolume(string tenant)
        {
            var query = await _client.QueryAsync(
                new QueryRequest
                {
                    TableName = Table.Products,
                    IndexName = Table.Indicies.CopyrightTokensByTradingVolume,
                    ScanIndexForward = true,
                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { "#secondary_market_availability",  Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey}
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { ":secondary_market_availability", new AttributeValue(Schema.CopyrightToken.GetSecondaryMarketAvailabilityPartitionKey(tenant, true)) }
                    },
                    KeyConditionExpression = "#secondary_market_availability = :secondary_market_availability",
                });

            if (query == null || query.Count == 0)
            {
                return Array.Empty<CopyrightToken>();
            }

            return query.Items.Select(x => MapProduct(x) as CopyrightToken);
        }

        /// <summary>
        /// Mapping Product
        /// </summary>
        /// <param name="item">Product Attribute List</param>
        /// <returns>ShareToken, Crypto etc Object</returns>
        private Product MapProduct(Dictionary<string, AttributeValue> item)
        {
            try
            {
                _ = item.TryGetValue(Schema.Product.CommonAttributes.Category, out var productCategory);
                _ = item.TryGetValue(Schema.Product.CommonAttributes.Id, out var productid);
                var category = !string.IsNullOrEmpty(productCategory?.S) ? productCategory.S : string.Empty;
                var productId = !string.IsNullOrEmpty(productid?.S) ? productid.S : string.Empty;

                if (!string.IsNullOrEmpty(category) && !string.IsNullOrEmpty(productId))
                {
                    var cultureInfo = CultureInfo.InvariantCulture;
                    _ = item.TryGetValue(Schema.Product.CommonAttributes.Color, out var productColor);
                    _ = item.TryGetValue(Schema.Product.CommonAttributes.ProductStatus, out var productStatusAttribute);
                    _ = item.TryGetValue(Schema.Product.CommonAttributes.InstrumentId, out var productInstrumentIdAttribute);

                    switch (category.Trim().ToUpper())
                    {
                        case "CRYPTO":
                            _ = item.TryGetValue(Schema.Crypto.Attributes.ExternalAssetId, out var cryptoExternalAssetId);

                            return Crypto.RestoreFromDatabase(
                                productId,
                                !string.IsNullOrEmpty(productColor?.S) ? productColor.S : string.Empty,
                                item.TryGetValue(Schema.Product.CommonAttributes.IsMinted, out var cr) && cr.BOOL,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productId,
                                int.TryParse(productStatusAttribute?.N, out var c) ? c : 0,
                                !string.IsNullOrEmpty(cryptoExternalAssetId?.S) ? cryptoExternalAssetId.S : string.Empty);

                        case "SIMPLE":
                            return Simple.RestoreFromDatabase(
                                productId,
                                !string.IsNullOrEmpty(productColor?.S) ? productColor.S : string.Empty,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productId,
                                int.TryParse(productStatusAttribute?.N, out var s) ? s : 0); 

                        case "FIAT":
                            return Fiat.RestoreFromDatabase(
                                productId,
                                !string.IsNullOrEmpty(productColor?.S) ? productColor.S : string.Empty,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productId,
                                int.TryParse(productStatusAttribute?.N, out var f) ? f : 0);

                        case "COPYRIGHTTOKEN":
                            _ = item.TryGetValue(Schema.CopyrightToken.Attributes.ExternalMusicId, out var externalMusicId);
                            _ = item.TryGetValue(Schema.CopyrightToken.Attributes.CreatorId, out var creatorId);
                            _ = item.TryGetValue(Schema.CopyrightToken.Attributes.Icon, out var icon);
                            _ = item.TryGetValue(Schema.CopyrightToken.Attributes.SubType, out var subType);
                            _ = item.TryGetValue(Schema.CopyrightToken.Attributes.Ownership, out var ownership);
                            _ = item.TryGetValue(Schema.CopyrightToken.Attributes.SongDetails, out var songDetails);

                            return CopyrightToken.RestoreFromDatabase(
                                productId,
                                !string.IsNullOrEmpty(externalMusicId?.S) ? externalMusicId.S : string.Empty,
                                !string.IsNullOrEmpty(creatorId?.S) ? creatorId.S : string.Empty,
                                !string.IsNullOrEmpty(icon?.S) ? icon.S : string.Empty,
                                !string.IsNullOrEmpty(productColor?.S) ? productColor.S : string.Empty,
                                item[Schema.Attributes.PartitionKey].S.Split('#').ToArray()[1],
                                !string.IsNullOrEmpty(subType?.S) ? subType.S : string.Empty,
                                !string.IsNullOrEmpty(ownership?.S) ? ownership.S : string.Empty,
                                decimal.Parse(item[Schema.CopyrightToken.Attributes.Amount].S, cultureInfo),
                                !string.IsNullOrEmpty(songDetails?.S) ? songDetails.S : string.Empty,
                                decimal.Parse(item[Schema.CopyrightToken.Attributes.AlreadyAuctionedAmount].S, cultureInfo),
                                bool.Parse(item[Schema.CopyrightToken.Attributes.IsAvailableForSecondaryMarket].S),
                                item.TryGetValue(Schema.Product.CommonAttributes.IsMinted, out var a) && a.BOOL,
                                item.TryGetValue(Schema.CopyrightToken.Attributes.TradingVolume, out var tradingVolume) ? decimal.Parse(tradingVolume?.N ?? "0", cultureInfo) : 0m,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : string.Empty,
                                int.TryParse(productStatusAttribute?.N, out var ct) ? ct : 0);

                        case "SHARETOKEN":
                            _ = item.TryGetValue(Schema.ShareToken.Attributes.Name, out var productName);
                            _ = item.TryGetValue(Schema.ShareToken.Attributes.Ticker, out var ticker);
                            _ = item.TryGetValue(Schema.ShareToken.Attributes.DocumentUrl, out var docURL);
                            _ = item.TryGetValue(Schema.ShareToken.Attributes.BlockchainErrorMessage, out var blockChainMsg);
                            _ = item.TryGetValue(Schema.ShareToken.Attributes.ExternalAssetId, out var shareTokenExternalAssetId);

                            return ShareToken.RestoreFromDatabase(
                                productId,
                                !string.IsNullOrEmpty(productName?.S) ? productName.S : string.Empty,
                                !string.IsNullOrEmpty(ticker?.S) ? ticker.S : string.Empty,
                                !string.IsNullOrEmpty(docURL?.S) ? docURL.S : string.Empty,
                                item.TryGetValue(Schema.Product.CommonAttributes.IsMinted, out var av) && av.BOOL,
                                !string.IsNullOrEmpty(productColor?.S) ? productColor.S : string.Empty,
                                item.TryGetValue(Schema.ShareToken.Attributes.IsDeployed, out var isDeployed) && isDeployed.BOOL,
                                item.TryGetValue(Schema.ShareToken.Attributes.IsFrozen, out var isFrozen) && isFrozen.BOOL,
                                !string.IsNullOrEmpty(blockChainMsg?.S) ? blockChainMsg.S : string.Empty,
                                item.TryGetValue(Schema.ShareToken.Attributes.TotalSupply, out var totalSupply) ? decimal.Parse(totalSupply.N, cultureInfo) : 0m,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productId,
                                int.TryParse(productStatusAttribute?.N, out var st) ? st : 0,
                                !string.IsNullOrEmpty(shareTokenExternalAssetId?.S) ? shareTokenExternalAssetId.S : string.Empty);
                    }
                }
                else
                {
                    _logger.LogError($"Product Mapping Failure due to missing product category and product id attribute with item is {string.Join(",", item)}");
                }
                return default;
            }
            catch (Exception ex)
            {
                var dynamoAttributes = JsonConvert.SerializeObject(item);
                _logger.LogError("Exception Ocurred and the message is  {Exception} and the serialized message is {Attribute}", ex, dynamoAttributes);
                throw;
            }
        }

        private static string GetProductCategory(Type type)
        {
            if (type == typeof(Crypto))
            {
                return "Crypto";
            }
            else if (type == typeof(CopyrightToken))
            {
                return "CopyrightToken";
            }
            else if (type == typeof(ShareToken))
            {
                return "ShareToken";
            }
            else if (type == typeof(Simple))
            {
                return "Simple";
            }
            else if (type == typeof(Fiat))
            {
                return "Fiat";
            }
            else
            {
                throw new Exception("Unsupported product category");
            }
        }
    }
}
