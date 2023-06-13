namespace Lyra.Persistence
{
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Lyra.Instruments;
    using Lyra.Persistence.Exceptions;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;

    public sealed class ProductWriteRepository : IProductWriteRepository
    {
        private const string ConditionalCheckFailed = "ConditionalCheckFailed";

        private readonly IAmazonDynamoDB client;
        private readonly ILogger _logger;

        public ProductWriteRepository(IAmazonDynamoDB client, ILogger<IProductWriteRepository> logger)
        {
            this.client = client ?? throw new ArgumentNullException(nameof(client));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task AddShareTokenProductAsync(string tenant, ShareToken shareToken)
        {
            var transactList = new List<TransactWriteItem>()
            {
                CreateShareTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, shareToken))
            };
            try
            {
                await client.Execute(null, transactList);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Share Token {shareToken.ProductId} already exists", ex);
                }

                throw;
            }
        }

        public async Task AddShareTokenProductWithInstrumentAsync(string tenant, ShareToken shareToken, Instrument instrument = null)
        {
            if (instrument == null)
            {
                instrument = new Instrument(shareToken.ProductId, shareToken.Name, shareToken.NumberOfDecimalPlaces);
            }
            var transactList = new List<TransactWriteItem>()
            {
                CreateInstrumentTransactWriteItem(Table.Products, GetDefinitions(tenant, instrument)),
                CreateShareTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, shareToken))
            };
            try
            {
                await client.Execute(null, transactList);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Share Token {shareToken.ProductId} already exists", ex);
                }

                throw;
            }
            catch (Exception ex)
            {
                var transactionlistInstruments = JsonConvert.SerializeObject(instrument);
                var transactionlistShareToken = JsonConvert.SerializeObject(shareToken);
                _logger.LogError(ex, $"Issue while creating share token with Instrument Id {shareToken.ProductId} for tenant {tenant}." +
                    $" Exception message is {ex}. Attribute Values are for instruments: {transactionlistInstruments} and products: {transactionlistShareToken}");
                throw;
            }
        }

        public async Task AddCryptoProductAsync(
          string tenant,
          Crypto crypto)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                CreateCryptoTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, crypto))
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Crypto {crypto.ProductId} already exists", ex);
                }

                throw;
            }
        }

        /// <summary>
        /// Adding SImple Product
        /// </summary>
        /// <param name="tenant"></param>
        /// <param name="simple"></param>
        /// <returns></returns>
        /// <exception cref="AlreadyExistsException"></exception>
        public async Task AddSimpleProductAsync(
          string tenant,
          Simple simple)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                CreateSimpleTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, simple))
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Simple {simple.ProductId} already exists", ex);
                }

                throw;
            }
        }


        /// <summary>
        /// Adding Fiat Product
        /// </summary>
        /// <param name="tenant"></param>
        /// <param name="fiat"></param>
        /// <returns></returns>
        /// <exception cref="AlreadyExistsException"></exception>
        public async Task AddFiatProductAsync(
          string tenant,
          Fiat fiat)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                CreateFiatTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, fiat))
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Fiat {fiat.ProductId} already exists", ex);
                }

                throw;
            }
        }

        public async Task AddCryptoProductWithInstrumentAsync(
            string tenant,
            Crypto crypto,
            Instrument instrument)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                CreateInstrumentTransactWriteItem(Table.Products, GetDefinitions(tenant, instrument)),
                CreateCryptoTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, crypto))
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Either Instrument {instrument.Id} or Crypto {crypto.ProductId} already exists", ex);
                }

                throw;
            }
        }

        public async Task AddSimpleProductWithInstrumentAsync(
          string tenant,
          Simple simple,
          Instrument instrument)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                CreateInstrumentTransactWriteItem(Table.Products, GetDefinitions(tenant, instrument)),
                CreateSimpleTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, simple))
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Either Instrument {instrument.Id} or Simple {simple.ProductId} already exists", ex);
                }

                throw;
            }
        }

        public async Task AddFiatProductWithInstrumentAsync(
        string tenant,
        Fiat fiat,
        Instrument instrument)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                CreateInstrumentTransactWriteItem(Table.Products, GetDefinitions(tenant, instrument)),
                CreateFiatTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, fiat))
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Either Instrument {instrument.Id} or Fiat {fiat.ProductId} already exists", ex);
                }

                throw;
            }
        }

        public async Task RemoveProductWithInstrumentAsync(string tenant, string id, string productType)
        {
            try
            {

                var transactItems = new List<TransactWriteItem>()
            {
                RemoveTransactWriteItem(Table.Products,
                    new Dictionary<string, AttributeValue>()
                    {
                        {
                            Schema.Attributes.PartitionKey,
                            new AttributeValue(Schema.Product.GetPrimaryPartitionKey(tenant, id))
                        },
                        {
                            Schema.Attributes.SortKey, new AttributeValue(Schema.Product.GetPrimarySortKey(productType))
                        },
                    }),
                RemoveTransactWriteItem(Table.Products,
                    new Dictionary<string, AttributeValue>
                    {
                        {
                            Schema.Attributes.PartitionKey,
                            new AttributeValue(Schema.Instrument.GetPartitionKey(tenant, id))
                        },
                        { Schema.Attributes.SortKey, new AttributeValue(Schema.Instrument.GetSortKey()) },
                    })
            };

                // TODO: try catch block needed? check instrument exists first? what else can go wrong?
                await client.Execute(null, transactItems);
            }
            catch (Exception ex)
            {
                _logger.LogError("Exception ocurred while copyright product & instrument deletion for tenant: {Tenant} and Exception is {Exception} with Id {ProductId}", tenant, ex, id);
            }
        }

        public async Task RemoveProductAsync(string tenant, string id, string productType)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                this.RemoveTransactWriteItem(Table.Products,
                    new Dictionary<string, AttributeValue>()
                    {
                        {
                            Schema.Attributes.PartitionKey,
                            new AttributeValue(Schema.Product.GetPrimaryPartitionKey(tenant, id))
                        },
                        {
                            Schema.Attributes.SortKey, new AttributeValue(Schema.Product.GetPrimarySortKey(productType))
                        },
                    })
            };

            // TODO: try catch block needed? check instrument exists first? what else can go wrong?
            await client.Execute(null, transactItems);
        }

        public async Task RemoveInstrumentAsync(string tenant, string id)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                RemoveTransactWriteItem(Table.Products,
                    new Dictionary<string, AttributeValue>
                    {
                        {
                            Schema.Attributes.PartitionKey,
                            new AttributeValue(Schema.Instrument.GetPartitionKey(tenant, id))
                        },
                        { Schema.Attributes.SortKey, new AttributeValue(Schema.Instrument.GetSortKey()) },
                    })
            };

            // TODO: try catch block needed? check instrument exists first? what else can go wrong?
            await client.Execute(null, transactItems);
        }

        /// <summary>
        /// Updating the Crypto Token
        /// </summary>
        /// <param name="tenant">Tenant id</param>
        /// <param name="crypto">Crypto Token Object</param>
        /// <returns></returns>
        public async Task UpdateCryptoTokenAsync(string tenant, Crypto crypto)
        {
            var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(
                                    Schema.Product.GetPrimaryPartitionKey(tenant, crypto.ProductId))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Product.GetPrimarySortKey(crypto.Category))
                            }
                        },
                        UpdateExpression =
                            "SET #is_minted = :is_minted,  #is_color = :is_color, #is_instrument = :is_instrument," +
                            " #crypto_external_asset_Id = :crypto_external_asset_Id",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#is_minted", Schema.Product.CommonAttributes.IsMinted },
                            { "#is_color", Schema.Product.CommonAttributes.Color },
                            { "#is_instrument", Schema.Product.CommonAttributes.InstrumentId },
                            { "#crypto_external_asset_Id", Schema.Crypto.Attributes.ExternalAssetId},
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":is_minted", new AttributeValue { BOOL = crypto.IsMinted } },
                            { ":is_color", new AttributeValue { S = crypto.Color } },
                            { ":is_instrument", new AttributeValue { S = crypto.InstrumentId } },
                            { ":crypto_external_asset_Id", new AttributeValue { S = crypto.ExternalAssetId } },
                        }
                    }
                }
            };

            await client.Execute(null, transactItems);
        }

        /// <summary>
        /// Updating the Simple Token
        /// </summary>
        /// <param name="tenant">Tenant id</param>
        /// <param name="simple">Simple Token Object</param>
        /// <returns></returns>
        public async Task UpdateSimpleTokenAsync(string tenant, Simple simple)
        {
            try
            {
                var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(
                                    Schema.Product.GetPrimaryPartitionKey(tenant, simple.ProductId))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Product.GetPrimarySortKey(simple.Category))
                            }
                        },
                        UpdateExpression =
                            "SET #is_color = :is_color, #is_instrument = :is_instrument",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#is_color", Schema.Product.CommonAttributes.Color },
                            { "#is_instrument", Schema.Product.CommonAttributes.InstrumentId },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":is_color", new AttributeValue { S = simple.Color } },
                            { ":is_instrument", new AttributeValue { S = simple.InstrumentId } },
                        }
                    }
                }
            };

                await client.Execute(null, transactItems);
            }
            catch (Exception ex)
            {
                var productInfo = JsonConvert.SerializeObject(simple);
                _logger.LogError(ex, $"Issue while updating simple token with Id {simple.ProductId} for tenant {tenant} and exception is:  {ex} with serialized message is {productInfo}");
                throw;
            }
        }

        /// <summary>
        /// Updating the Fiat Token
        /// </summary>
        /// <param name="tenant">Tenant id</param>
        /// <param name="fiat">Fiat Token Object</param>
        /// <returns></returns>
        public async Task UpdateFiatTokenAsync(string tenant, Fiat fiat)
        {
            try
            {
                var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(
                                    Schema.Product.GetPrimaryPartitionKey(tenant, fiat.ProductId))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Product.GetPrimarySortKey(fiat.Category))
                            }
                        },
                        UpdateExpression =
                            "SET #is_color = :is_color, #is_instrument = :is_instrument",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#is_color", Schema.Product.CommonAttributes.Color },
                            { "#is_instrument", Schema.Product.CommonAttributes.InstrumentId },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":is_color", new AttributeValue { S = fiat.Color } },
                            { ":is_instrument", new AttributeValue { S = fiat.InstrumentId } },
                        }
                    }
                }
            };

                await client.Execute(null, transactItems);
            }
            catch (Exception ex)
            {
                var productInfo = JsonConvert.SerializeObject(fiat);
                _logger.LogError(ex, $"Issue while updating fiat token with Id {fiat.ProductId} for tenant {tenant} and exception is: {ex} with serialized message is {productInfo}");
                throw;
            }
        }
        
        public async Task UpdateShareTokenAsync(string tenant, ShareToken shareToken, string idempotencyToken = null)
        {
            try
            {
                var transactItems = new List<TransactWriteItem>
                        {
                           new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(
                                    Schema.Product.GetPrimaryPartitionKey(tenant, shareToken.ProductId))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Product.GetPrimarySortKey(shareToken.Category))
                            }
                        },
                    }
                }
            };

                transactItems.Single().Update.UpdateExpression += $"SET ";
                ApplyUpdateExpression(shareToken.IsMinted, Schema.Product.CommonAttributes.IsMinted, "is_minted", transactItems);
                ApplyUpdateExpression(shareToken.IsDeployed, Schema.ShareToken.Attributes.IsDeployed, "is_deployed", transactItems);
                ApplyUpdateExpression(shareToken.IsFrozen, Schema.ShareToken.Attributes.IsFrozen, "is_frozen", transactItems);
                ApplyUpdateExpression(shareToken.TotalSupply, Schema.ShareToken.Attributes.TotalSupply, "total_supply", transactItems);
                ApplyUpdateExpression(shareToken.Color, Schema.Product.CommonAttributes.Color, "share_token_color", transactItems);
                ApplyUpdateExpression(shareToken.Name, Schema.ShareToken.Attributes.Name, "share_token_name", transactItems);
                ApplyUpdateExpression(shareToken.Ticker, Schema.ShareToken.Attributes.Ticker, "share_token_ticker", transactItems);
                ApplyUpdateExpression(shareToken.InstrumentId, Schema.Product.CommonAttributes.InstrumentId, "share_token_instrument_id", transactItems);
                ApplyUpdateExpression(shareToken.DocumentUrl, Schema.ShareToken.Attributes.DocumentUrl, "share_token_document_url", transactItems);
                ApplyUpdateExpression(shareToken.ExternalAssetId, Schema.ShareToken.Attributes.ExternalAssetId, "share_token_external_asset_Id", transactItems);
                ApplyUpdateExpression(shareToken.BlockchainErrorMessage, Schema.ShareToken.Attributes.BlockchainErrorMessage, "blockchain_error_message", transactItems);
                transactItems.Single().Update.UpdateExpression = transactItems.Single().Update.UpdateExpression.TrimEnd(',');

                await client.Execute(idempotencyToken, transactItems);
            }
            catch (Exception ex)
            {
                var productInfo = JsonConvert.SerializeObject(shareToken);
                _logger.LogError(ex, $"Issue while updating share token with Id {shareToken.ProductId} for tenant {tenant} and exception is: {ex} with serialized message is {productInfo}");
                throw;
            }
        }

        /// <summary>
        /// Dynamic Update Expression
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldValue">Expression Atribute Names</param>
        /// <param name="fieldName"></param>
        /// <param name="literal"></param>
        /// <param name="transactItems"></param>
        private void ApplyUpdateExpression<T>(T fieldValue, string fieldName, string literal, List<TransactWriteItem> transactItems)
        {
            var updateExpressionAttributeValue = GetAttributeValue(fieldValue);

            if (updateExpressionAttributeValue == null)
            {
                return;
            }

            transactItems.Single().Update.UpdateExpression += $"#{literal} = :{literal} ,";
            transactItems.Single().Update.ExpressionAttributeNames[$"#{literal}"] = fieldName;
            transactItems.Single().Update.ExpressionAttributeValues[$":{literal}"] = updateExpressionAttributeValue;
        }

        private AttributeValue GetAttributeValue<T>(T fieldValue)
        {
            AttributeValue updateExpressionAttributeValue = null;
            switch (Type.GetTypeCode(typeof(T)))
            {
                case TypeCode.Boolean:
                    updateExpressionAttributeValue = new AttributeValue { BOOL = Convert.ToBoolean(fieldValue, CultureInfo.InvariantCulture) };
                    break;
                case TypeCode.Decimal:
                    var itemValue = Convert.ToString(fieldValue, CultureInfo.InvariantCulture);
                    if (!string.IsNullOrEmpty(itemValue))
                    {
                        updateExpressionAttributeValue = new AttributeValue { N = itemValue };
                    }

                    break;
                case TypeCode.String:
                    var item = Convert.ToString(fieldValue, CultureInfo.InvariantCulture);
                    if (!string.IsNullOrEmpty(item))
                    {
                        updateExpressionAttributeValue = new AttributeValue { S = item };
                    }
                    break;
                default:
                    this._logger.LogError($"Unknown attribute type {typeof(T)}");
                    break;
            }

            return updateExpressionAttributeValue;
        }

        public async Task UpdateCopyrightTokenAsync(string tenant, CopyrightToken copyrightToken)
        {
            var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(
                                    Schema.Product.GetPrimaryPartitionKey(tenant, copyrightToken.ProductId))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Product.GetPrimarySortKey(copyrightToken.Category))
                            }
                        },
                        UpdateExpression =
                            "SET " +
                            "#is_available_for_secondary_auction = :is_available_for_secondary_auction, " +
                            "#auctioned_amount = :auctioned_amount, " +
                            "#is_available_for_secondary_auction_primary_key=:is_available_for_secondary_auction_primary_key, " +
                            "#song_details=:song_details, " +
                            "#is_minted=:is_minted, " +
                            "#trading_volume=:trading_volume, " +
                            "#secondary_market_availability_trading_volume_sort_key=:secondary_market_availability_trading_volume_sort_key",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            {
                                "#is_available_for_secondary_auction",
                                Schema.CopyrightToken.Attributes.IsAvailableForSecondaryMarket
                            },
                            { "#is_minted", Schema.Product.CommonAttributes.IsMinted },
                            {
                                "#is_available_for_secondary_auction_primary_key",
                                Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey
                            },
                            { "#auctioned_amount", Schema.CopyrightToken.Attributes.AlreadyAuctionedAmount },
                            { "#trading_volume", Schema.CopyrightToken.Attributes.TradingVolume },
                            { "#secondary_market_availability_trading_volume_sort_key", Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityTradingVolumeSortKey },
                            { "#song_details", Schema.CopyrightToken.Attributes.SongDetails }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":is_available_for_secondary_auction",
                                new AttributeValue(copyrightToken.IsAvailableAtSecondaryMarket.ToString())
                            },
                            { ":is_minted", new AttributeValue { BOOL = copyrightToken.IsMinted } },
                            {
                                ":is_available_for_secondary_auction_primary_key",
                                new AttributeValue(
                                    Schema.CopyrightToken.GetSecondaryMarketAvailabilityPartitionKey(tenant,
                                        copyrightToken.IsAvailableAtSecondaryMarket))
                            },
                            {
                                ":auctioned_amount",
                                new AttributeValue(
                                    copyrightToken.AlreadyAuctionedAmount.ToString(CultureInfo.InvariantCulture))
                            },
                            {
                                ":trading_volume",
                                new AttributeValue
                                {
                                    N = copyrightToken.TradingVolume.ToString(CultureInfo.InvariantCulture)
                                }
                            },
                            {
                                ":secondary_market_availability_trading_volume_sort_key",
                                new AttributeValue(
                                    Schema.CopyrightToken.GetSecondaryMarketAvailabilityTradingVolumeSortKey(copyrightToken.TradingVolume))
                            },
                            {
                                ":song_details",
                                new AttributeValue(JsonConvert.SerializeObject(copyrightToken.SongDetails))
                            }
                        }
                    }
                }
            };

            await client.Execute(null, transactItems);
        }

        public async Task UpdateProductAsMinted(string tenant, Product product)
        {
            var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(
                                    Schema.Product.GetPrimaryPartitionKey(tenant, product.ProductId))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Product.GetPrimarySortKey(product.Category))
                            }
                        },
                        UpdateExpression =
                            "SET #is_minted=:is_minted",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#is_minted", Schema.Product.CommonAttributes.IsMinted }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":is_minted", new AttributeValue { BOOL = true } }
                        },
                        ConditionExpression = "#is_minted <> :is_minted"
                    }
                }
            };

            await client.Execute(null, transactItems);
        }

        /// <summary>
        /// Update the Product Status
        /// </summary>
        /// <param name="tenant">tenant Id</param>
        /// <param name="product">Product</param>
        /// <param name="productStatus">Product</param>
        /// <returns></returns>
        public async Task UpdateProductStatusAsync(string tenant, Product product, int productStatus)
        {
            var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(
                                    Schema.Product.GetPrimaryPartitionKey(tenant, product.ProductId))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Product.GetPrimarySortKey(product.Category))
                            }
                        },
                        UpdateExpression =
                           "SET #status = :status, #index_key = :index_key, #sort_key = :sort_key",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#index_key", Schema.Product.CommonAttributes.ProductStatusPartitionKey },
                            { "#sort_key", Schema.Product.CommonAttributes.ProductStatusSortKey },
                            { "#status", Schema.Product.CommonAttributes.ProductStatus },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":index_key", new AttributeValue { S = Schema.Product.GetProductStatusPartitionKey(tenant, productStatus) } },
                            { ":sort_key", new AttributeValue { S = Schema.Product.GetProductStatusSortKey(product.ProductId, product.Category) } },
                            { ":status", new AttributeValue { N = productStatus.ToString()  } },
                        },
                    }
                }
            };

            await client.Execute(null, transactItems);
        }


        /// <summary>
        /// Update the Instrument Status
        /// </summary>
        /// <param name="tenant">tenant Id</param>
        /// <param name="id">instrument id</param>
        /// <param name="id">instrument status</param>
        /// <returns></returns>
        public async Task UpdateInstrumentStatusAsync(string tenant, string id, int instrumentStatus)
        {
            var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(Schema.Instrument.GetPartitionKey(tenant, id))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Instrument.GetSortKey())
                            }
                        },
                        UpdateExpression =
                            "SET #status = :status, #index_key = :index_key, #sort_key = :sort_key",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#index_key", Schema.Instrument.Attributes.InstrumentStatusPartitionKey },
                            { "#sort_key", Schema.Instrument.Attributes.InstrumentStatusSortKey },
                            { "#status", Schema.Instrument.Attributes.InstrumentStatus },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":index_key", new AttributeValue { S = Schema.Instrument.GetInstrumentStatusPartitionKey(tenant, instrumentStatus) } },
                            { ":sort_key", new AttributeValue { S = Schema.Instrument.GetInstrumentStatusSortKey(id) } },
                            { ":status", new AttributeValue { N = instrumentStatus.ToString() } },
                        }
                    }
                }
            };

            await client.Execute(null, transactItems);
        }


        /// <summary>
        /// Update the Instrument 
        /// </summary>
        /// <param name="tenant">tenant Id</param>
        /// <param name="id">instrument id</param>
        /// <param name="id">instrument status</param>
        /// <returns></returns>
        public async Task UpdateInstrumentAsync(string tenant, string id, Instrument request)
        {
            try
            {
                var transactItems = new List<TransactWriteItem>
            {
                new TransactWriteItem
                {
                    Update = new Update
                    {
                        TableName = Table.Products,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue(Schema.Instrument.GetPartitionKey(tenant, id))
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue(Schema.Instrument.GetSortKey())
                            }
                        },
                         UpdateExpression =
                            "SET #is_instrument_name=:is_instrument_name, #is_instrument_decimal_places=:is_instrument_decimal_places",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#is_instrument_name", Schema.Instrument.Attributes.Name },
                            { "#is_instrument_decimal_places", Schema.Instrument.Attributes.NumberOfDecimalPlaces },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":is_instrument_name", new AttributeValue { S = request.Name } },
                            { ":is_instrument_decimal_places", new AttributeValue { N = request.NumberOfDecimalPlaces.ToString() } }
                        },
                    }
                }
            };

                await client.Execute(null, transactItems);
            }
            catch (Exception ex)
            {
                var instrumentInfo = JsonConvert.SerializeObject(request);
                _logger.LogError(ex, $"Issue while updating instrument with Id {id} for tenant {tenant} and exception is: {ex} with serialized message is {instrumentInfo}");
                throw;
            }
        }
        public async Task AddCopyrightTokenWithInstrumentAsync(
            string tenant,
            CopyrightToken copyrightToken,
            Instrument instrument)
        {
            var transactItems = new List<TransactWriteItem>()
            {
                CreateInstrumentTransactWriteItem(Table.Products, GetDefinitions(tenant, instrument)),
                CreateCopyrightTokenTransactWriteItem(Table.Products, GetDefinitions(tenant, copyrightToken))
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index >= 0)
                {
                    throw new AlreadyExistsException(
                        $"Either Instrument {instrument.Id} or Copyright Token {copyrightToken.ProductId} already exists",
                        ex);
                }

                throw;
            }
        }

        public async Task AddInstrumentAsync(string tenant, Instrument instrument)
        {
            var tableName = Table.Products;

            var transactItems = new List<TransactWriteItem>()
            {
                new TransactWriteItem()
                {
                    Put = new Put()
                    {
                        TableName = tableName,
                        Item =GetDefinitions(tenant,instrument),
                        ConditionExpression = $"attribute_not_exists({Schema.Attributes.PartitionKey})"
                    }
                }
            };

            try
            {
                await client.Execute(null, transactItems);
            }
            catch (TransactionCanceledException ex) when (ex.Data.Contains(typeof(TransactionCanceledException)))
            {
                var index = Array.IndexOf((string[])ex.Data[typeof(TransactionCanceledException)],
                    ConditionalCheckFailed);

                if (index == 0)
                {
                    throw new AlreadyExistsException($"Instrument with id {instrument.Id} already exists");
                }

                throw;
            }
        }

        private TransactWriteItem CreateCryptoTokenTransactWriteItem(string tableName,
           Dictionary<string, AttributeValue> definitionItems)
        {
            return new TransactWriteItem()
            {
                Put = new Put()
                {
                    TableName = tableName,
                    Item = definitionItems,
                    ConditionExpression = $"attribute_not_exists({Schema.Attributes.PartitionKey})"
                }
            };
        }

        private TransactWriteItem CreateSimpleTokenTransactWriteItem(string tableName,
        Dictionary<string, AttributeValue> definitionItems)
        {
            return new TransactWriteItem()
            {
                Put = new Put()
                {
                    TableName = tableName,
                    Item = definitionItems,
                    ConditionExpression = $"attribute_not_exists({Schema.Attributes.PartitionKey})"
                }
            };
        }

        private TransactWriteItem CreateFiatTokenTransactWriteItem(string tableName,
       Dictionary<string, AttributeValue> definitionItems)
        {
            return new TransactWriteItem()
            {
                Put = new Put()
                {
                    TableName = tableName,
                    Item = definitionItems,
                    ConditionExpression = $"attribute_not_exists({Schema.Attributes.PartitionKey})"
                }
            };
        }

        private TransactWriteItem CreateCopyrightTokenTransactWriteItem(string tableName,
            Dictionary<string, AttributeValue> definitionItems)
        {
            return new TransactWriteItem()
            {
                Put = new Put()
                {
                    TableName = tableName,
                    Item = definitionItems,
                    ConditionExpression = $"attribute_not_exists({Schema.Attributes.PartitionKey})"
                }
            };
        }

        private TransactWriteItem CreateShareTokenTransactWriteItem(string tableName,
            Dictionary<string, AttributeValue> definitionItems)
        {
            return new TransactWriteItem()
            {
                Put = new Put()
                {
                    TableName = tableName,
                    Item = definitionItems,
                    ConditionExpression = $"attribute_not_exists({Schema.Attributes.PartitionKey})"
                }
            };
        }

        private TransactWriteItem CreateInstrumentTransactWriteItem(string tableName,
            Dictionary<string, AttributeValue> definitionItems)
        {
            return new TransactWriteItem()
            {
                Put = new Put()
                {
                    TableName = tableName,
                    Item = definitionItems,
                    ConditionExpression = $"attribute_not_exists({Schema.Attributes.PartitionKey})"
                }
            };
        }

        private TransactWriteItem RemoveTransactWriteItem(string tableName,
            Dictionary<string, AttributeValue> definitionItems)
        {
            return new TransactWriteItem()
            {
                Delete = new Delete()
                {
                    TableName = tableName,
                    Key = definitionItems
                }
            };
        }

        private static Dictionary<string, AttributeValue> GetDefaultDefinitions(string tenant, Product product)
        {
            return new Dictionary<string, AttributeValue>()
            {
                {
                    Schema.Attributes.PartitionKey,new AttributeValue(Schema.Product.GetPrimaryPartitionKey(tenant, product.ProductId))
                },
                { Schema.Attributes.SortKey, new AttributeValue(Schema.Product.GetPrimarySortKey(product.Category)) },
                {
                    Schema.Product.CommonAttributes.ProductPartitionKey,new AttributeValue(Schema.Product.GetProductPartitionKey(tenant))
                },
                {
                    Schema.Product.CommonAttributes.ProductSortKey,new AttributeValue(Schema.Product.GetProductSortKey(product.Category, product.ProductId))
                },
                { Schema.Product.CommonAttributes.Id, new AttributeValue(product.ProductId) },
                { Schema.Product.CommonAttributes.Category, new AttributeValue(product.Category.Trim().ToUpper()) },
                { Schema.Product.CommonAttributes.Color, new AttributeValue(string.IsNullOrEmpty(product.Color) ? string.Empty: product.Color) },
                { Schema.Product.CommonAttributes.InstrumentId, new AttributeValue(string.IsNullOrEmpty(product.InstrumentId) ? string.Empty: product.InstrumentId) },
                { Schema.Product.CommonAttributes.ProductStatus, new AttributeValue() { N = product.ProductStatus.ToString() } }
            };
        }

        public static Dictionary<string, AttributeValue> GetDefinitions(string tenant, Crypto product)
        {
            return new Dictionary<string, AttributeValue>(GetDefaultDefinitions(tenant, product))
            {
                { Schema.Product.CommonAttributes.IsMinted, new AttributeValue() { BOOL = product.IsMinted } },
                { Schema.Crypto.Attributes.ExternalAssetId, new AttributeValue(string.IsNullOrEmpty(product.ExternalAssetId) ? string.Empty: product.ExternalAssetId) },
                { Schema.Product.CommonAttributes.ProductStatusPartitionKey, new AttributeValue(Schema.Product.GetProductStatusPartitionKey(tenant,GetStatus(product.ProductStatus))) },
                { Schema.Product.CommonAttributes.ProductStatusSortKey, new AttributeValue(Schema.Product.GetProductStatusSortKey(product.Category,product.ProductId)) },
            };
        }

        public static Dictionary<string, AttributeValue> GetDefinitions(string tenant, Simple product)
        {
            return new Dictionary<string, AttributeValue>(GetDefaultDefinitions(tenant, product))
            {
                { Schema.Product.CommonAttributes.ProductStatusPartitionKey, new AttributeValue(Schema.Product.GetProductStatusPartitionKey(tenant,GetStatus(product.ProductStatus))) },
                { Schema.Product.CommonAttributes.ProductStatusSortKey, new AttributeValue(Schema.Product.GetProductStatusSortKey(product.Category,product.ProductId)) },
            };
        }

        public static Dictionary<string, AttributeValue> GetDefinitions(string tenant, Fiat product)
        {
            return new Dictionary<string, AttributeValue>(GetDefaultDefinitions(tenant, product))
            {
                { Schema.Product.CommonAttributes.ProductStatusPartitionKey, new AttributeValue(Schema.Product.GetProductStatusPartitionKey(tenant,GetStatus(product.ProductStatus))) },
                { Schema.Product.CommonAttributes.ProductStatusSortKey, new AttributeValue(Schema.Product.GetProductStatusSortKey(product.Category,product.ProductId)) },
            };
        }

        private static Dictionary<string, AttributeValue> GetDefinitions(string tenant, ShareToken shareToken)
        {
            return new Dictionary<string, AttributeValue>(GetDefaultDefinitions(tenant, shareToken))
            {
                { Schema.Product.CommonAttributes.IsMinted, new AttributeValue() { BOOL = shareToken.IsMinted } },
                { Schema.ShareToken.Attributes.Name, new AttributeValue(shareToken.Name) },
                { Schema.ShareToken.Attributes.Ticker, new AttributeValue(shareToken.Ticker) },
                { Schema.ShareToken.Attributes.DocumentUrl, new AttributeValue(shareToken.DocumentUrl) },
                { Schema.ShareToken.Attributes.IsDeployed, new AttributeValue() { BOOL = shareToken.IsDeployed } },
                { Schema.ShareToken.Attributes.IsFrozen, new AttributeValue() { BOOL = shareToken.IsFrozen } },
                {
                    Schema.ShareToken.Attributes.BlockchainErrorMessage,
                    new AttributeValue(string.IsNullOrEmpty(shareToken.BlockchainErrorMessage)
                        ? string.Empty
                        : shareToken.BlockchainErrorMessage)
                },
                {
                    Schema.ShareToken.Attributes.TotalSupply,
                    new AttributeValue() { N = shareToken.TotalSupply.ToString(CultureInfo.InvariantCulture) }
                },
                { Schema.ShareToken.Attributes.ExternalAssetId, new AttributeValue(string.IsNullOrEmpty(shareToken.ExternalAssetId) ? string.Empty: shareToken.ExternalAssetId) },
                { Schema.Product.CommonAttributes.ProductStatusPartitionKey, new AttributeValue(Schema.Product.GetProductStatusPartitionKey(tenant,GetStatus(shareToken.ProductStatus))) },
                { Schema.Product.CommonAttributes.ProductStatusSortKey, new AttributeValue(Schema.Product.GetProductStatusSortKey(shareToken.Category,shareToken.ProductId)) },
            };
        }

        public static Dictionary<string, AttributeValue> GetDefinitions(string tenant, Instrument instrument)
        {
            return new Dictionary<string, AttributeValue>
            {
                { Schema.Attributes.PartitionKey,new AttributeValue(Schema.Instrument.GetPartitionKey(tenant, instrument.Id))},
                { Schema.Attributes.SortKey, new AttributeValue(Schema.Instrument.GetSortKey()) },
                { Schema.Instrument.Attributes.Id, new AttributeValue(instrument.Id) },
                { Schema.Instrument.Attributes.Name, new AttributeValue(instrument.Name) },
                { Schema.Instrument.Attributes.InstrumentStatus, new AttributeValue {N =instrument.InstrumentStatus.ToString()}},
                { Schema.Instrument.Attributes.NumberOfDecimalPlaces, new AttributeValue {N = instrument.NumberOfDecimalPlaces.ToString()} },
                { Schema.Instrument.Attributes.InstrumentTenantPartitionKey,new AttributeValue(Schema.Instrument.GetTenantPartitionKey(tenant)) },
                { Schema.Instrument.Attributes.InstrumentTenantSortKey,new AttributeValue(Schema.Instrument.GetTenantSortKey(instrument.Id))},
                { Schema.Instrument.Attributes.InstrumentStatusPartitionKey, new AttributeValue(Schema.Instrument.GetInstrumentStatusPartitionKey(tenant,GetStatus(instrument.InstrumentStatus))) },
                { Schema.Instrument.Attributes.InstrumentStatusSortKey, new AttributeValue(Schema.Instrument.GetInstrumentStatusSortKey(instrument.Id)) },
            };
        }

        public static Dictionary<string, AttributeValue> GetDefinitions(string tenant, CopyrightToken product)
        {
            return new Dictionary<string, AttributeValue>(GetDefaultDefinitions(tenant, product))
            {
                { Schema.Product.CommonAttributes.IsMinted, new AttributeValue() { BOOL = product.IsMinted } },
                {
                    Schema.CopyrightToken.Attributes.ExternalMusicIdPartitionKey,
                    new AttributeValue(
                        Schema.CopyrightToken.GetExternalMusicIdPartitionKey(tenant, product.ExternalMusicId))
                },
                {
                    Schema.CopyrightToken.Attributes.ExternalMusicIdSortKey,
                    new AttributeValue(
                        Schema.CopyrightToken.GetExternalMusicIdSortKey(product.ExternalMusicId, product.SubType))
                },

                {
                    Schema.CopyrightToken.Attributes.CreatorIdPartitionKey,
                    new AttributeValue(Schema.CopyrightToken.GetCreatorIdPartitionKey(tenant, product.CreatorId))
                },
                {
                    Schema.CopyrightToken.Attributes.CreatorIdSortKey,
                    new AttributeValue(Schema.CopyrightToken.GetCreatorIdSortKey(product.CreatorId, product.ProductId))
                },

                {
                    Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey,
                    new AttributeValue(
                        Schema.CopyrightToken.GetSecondaryMarketAvailabilityPartitionKey(tenant,
                            product.IsAvailableAtSecondaryMarket))
                },
                {
                    Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilitySortKey,
                    new AttributeValue(Schema.CopyrightToken.GetSecondaryMarketAvailabilitySortKey(product.ProductId))
                },
                {
                    Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityTradingVolumeSortKey,
                    new AttributeValue(Schema.CopyrightToken.GetSecondaryMarketAvailabilityTradingVolumeSortKey(product.TradingVolume))
                },
                {
                    Schema.CopyrightToken.Attributes.Amount,
                    new AttributeValue(product.Amount.ToString(CultureInfo.InvariantCulture))
                },
                { Schema.CopyrightToken.Attributes.Icon, new AttributeValue(product.Icon) },
                { Schema.CopyrightToken.Attributes.Ownership, new AttributeValue(product.Ownership) },
                { Schema.CopyrightToken.Attributes.CreatorId, new AttributeValue(product.CreatorId) },
                {
                    Schema.CopyrightToken.Attributes.SongDetails,
                    new AttributeValue(JsonConvert.SerializeObject(product.SongDetails))
                },
                { Schema.CopyrightToken.Attributes.SubType, new AttributeValue(product.SubType.ToString()) },
                {
                    Schema.CopyrightToken.Attributes.AlreadyAuctionedAmount,
                    new AttributeValue(product.AlreadyAuctionedAmount.ToString(CultureInfo.InvariantCulture))
                },
                { Schema.CopyrightToken.Attributes.ExternalMusicId, new AttributeValue(product.ExternalMusicId) },
                {
                    Schema.CopyrightToken.Attributes.IsAvailableForSecondaryMarket,
                    new AttributeValue(product.IsAvailableAtSecondaryMarket.ToString())
                },
                {
                    Schema.CopyrightToken.Attributes.TradingVolume,
                    new AttributeValue
                    {
                        N = product.TradingVolume.ToString(CultureInfo.InvariantCulture)
                    }
                },
                {
                    Schema.Product.CommonAttributes.ProductStatusPartitionKey,
                    new AttributeValue(Schema.Product.GetProductStatusPartitionKey(tenant, (int)ProductsStatus.Enabled))
                },
                {
                    Schema.Product.CommonAttributes.ProductStatusSortKey,
                    new AttributeValue(Schema.Product.GetProductStatusSortKey(product.ProductId, product.Category))
                },
            };
        }

        /// <summary>
        /// Get the Product or Instrument Status
        /// </summary>
        /// <param name="status"></param>
        /// <returns></returns>
        public static int GetStatus(int status)
        {
            switch (status)
            {
                case 0:
                    status = (int)InstrumentStatus.Enabled;
                    break;
                case 1:
                    status = (int)InstrumentStatus.Disabled;
                    break;
            }
            return status;
        }
    }
}
