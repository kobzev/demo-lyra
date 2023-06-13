namespace Lyra.Persistence
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Lyra.Instruments;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using Newtonsoft.Json;

    public class DynamoDbTableProvisioner
    {
        private readonly IAmazonDynamoDB client;

        private readonly ILogger logger;

        public DynamoDbTableProvisioner(IAmazonDynamoDB client, ILogger<DynamoDbTableProvisioner> logger)
        {
            this.client = client ?? throw new ArgumentNullException(nameof(client));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task ProvisionLykke(List<string> tenants, string tableName,
            CancellationToken cancellationToken = default)
        {
            await ProvisionTable(tableName, cancellationToken);
            await FillTradingVolume(tableName);
            await CreateCopyrightTokensByTradingVolumeIfNotExist(tableName);
            await RemoveOldIndicies(tableName);
            await ProvisionLykkeInstruments(tenants, cancellationToken);
            await MigrateContributors(tableName);
            await MigrateMintedField(tableName);
            await PopulateInstrumentIdForExistingProducts(tableName);
            await MigrateProducts(tableName);
            await ProvisionInstrumentsIndexValues(tableName);
            await ProvisionInstrumentStatusIndexValues(tableName);
            await ProvisionProductCategoryValues(tableName);
            await ProvisionProductStatusIndexValues(tableName);
            //await RemoveCopyrightTokensWithInstruments(tenants); (Run Locally .May Lead Performance Issues)
            //await this.RemoveNumberOfDecimalPlacesFieldFromProductItems(tableName);
            //await this.PopulateNumberOfDecimalPlacesForExistingInstruments(tableName);
        }

        private async Task<string> ProvisionTable(string tableName, CancellationToken cancellationToken)
        {
            this.logger.LogInformation("Creating table for Lyra");

            try
            {
                tableName = await this.CreateTable(tableName, this.logger, cancellationToken);
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Could not create table");
            }

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new Exception($"Could not create table for Lyra");
            }

            this.logger.LogInformation("Created table for Lyra");
            return tableName;
        }

        private async Task ProvisionLykkeInstruments(List<string> tenants,
            CancellationToken cancellationToken = default)
        {
            var repository = new ProductWriteRepository(client, NullLogger<ProductWriteRepository>.Instance);
            var instrumentStatus = (int)InstrumentStatus.Enabled;

            foreach (var tenant in tenants)
            {
                var fiatInstruments = new[]
                {
                    new Instrument("currency.fiat.CHF", "Swiss Franc",instrumentStatus, 2),
                    new Instrument("currency.fiat.EUR", "Euro",instrumentStatus, 2),
                    new Instrument("currency.fiat.GBP", "Pound Sterling",instrumentStatus, 2),
                    new Instrument("currency.fiat.USD", "United States Dollar",instrumentStatus, 2),
                };

                var simpleInstruments = new[]
                {
                    new Instrument("currency.simple.CHF", "Swiss Franc (Simple)",instrumentStatus, 2),
                    new Instrument("currency.simple.EUR", "Euro (Simple)",instrumentStatus, 2),
                    new Instrument("currency.simple.GBP", "Pound Sterling (Simple)",instrumentStatus, 2),
                    new Instrument("currency.simple.USD", "United States Dollar (Simple)",instrumentStatus, 2),
                };

                var instrumentsAndCryptos = new List<(Instrument, Crypto)>
                {
                    (
                        new Instrument("currency.crypto.BTC", "Bitcoin",instrumentStatus, 6),
                        new Crypto("currency.crypto.BTC", "#2D3778")
                    ),
                    (
                        new Instrument("currency.crypto.BCH", "Bitcoin Cash",instrumentStatus, 6),
                        new Crypto("currency.crypto.BCH", "#4A5490")
                    ),
                    (
                        new Instrument("currency.crypto.ETH", "Ethereum",instrumentStatus, 6),
                        new Crypto("currency.crypto.ETH", "#6DAAB0")
                    ),
                    (
                        new Instrument("currency.crypto.LTC", "Litecoin",instrumentStatus, 6),
                        new Crypto("currency.crypto.LTC", "#6A4C69")
                    ),
                    (
                        new Instrument("currency.crypto.DOGE", "Dogecoin",instrumentStatus, 6),
                        new Crypto("currency.crypto.DOGE", "#C3A634")
                    ),
                    (
                        new Instrument("currency.crypto.USDT", "USDT",instrumentStatus, 2),
                        new Crypto("currency.crypto.USDT", "#C3A634")
                    ),
                    (
                        new Instrument("currency.crypto.USDC", "USDC",instrumentStatus, 2),
                        new Crypto("currency.crypto.USDC", "#C3A635")
                    ),
                    (
                        new Instrument("currency.crypto.BNB", "Binance",instrumentStatus, 6),
                        new Crypto("currency.crypto.BNB", "#C3A635")
                    ),
                    (
                        new Instrument("currency.crypto.MATIC", "Polygon",instrumentStatus, 6),
                        new Crypto("currency.crypto.MATIC", "#3E8726")
                    ),
                };

                if (tenant == "rhino")
                {
                    instrumentsAndCryptos = new List<(Instrument, Crypto)>
                    {
                        (
                            new Instrument("currency.crypto.DGD", "Alpenbrevet 1.",instrumentStatus, 0),
                            new Crypto("currency.crypto.DGD", "#C3A634")
                        ),
                        (
                            new Instrument("currency.crypto.CTC", "Alpenbrevet 3.",instrumentStatus, 0),
                            new Crypto("currency.crypto.CTC", "#CD7F32")
                        ),
                    };
                }

                if (tenant == "hippo")
                {
                    instrumentsAndCryptos = new List<(Instrument, Crypto)>
                    {
                        (
                            new Instrument("currency.crypto.BTC", "Bitcoin",instrumentStatus, 6),
                            new Crypto("currency.crypto.BTC", "#2D3778")
                        ),
                        (
                            new Instrument("currency.crypto.BCH", "Bitcoin Cash",instrumentStatus, 6),
                            new Crypto("currency.crypto.BCH", "#4A5490")
                        ),
                        (
                            new Instrument("currency.crypto.ETH", "Ethereum",instrumentStatus, 6),
                            new Crypto("currency.crypto.ETH", "#6DAAB0")
                        ),
                        (
                            new Instrument("currency.crypto.LTC", "Litecoin",instrumentStatus, 6),
                            new Crypto("currency.crypto.LTC", "#6A4C69")
                        ),
                    };
                }

                await ProvisionFiat(repository, tenant, fiatInstruments);

                await ProvisionSimple(repository, tenant, simpleInstruments);

                await this.ProvisionCryptosWithInstruments(repository, tenant, instrumentsAndCryptos);

                this.logger.LogInformation("Provisioned instruments for Lyra");
            }
        }

        private async Task ProvisionCryptosWithInstruments(ProductWriteRepository repository, string tenant,
            List<(Instrument, Crypto)> instrumentsAndCryptos)
        {
            foreach (var (instrument, crypto) in instrumentsAndCryptos)
            {
                try
                {
                    logger.LogInformation(
                        $"Creating crypto product ({crypto.ProductId}) in Lyra tenant ({tenant})");
                    await repository.AddCryptoProductWithInstrumentAsync(tenant, crypto, instrument);
                    logger.LogDebug(
                        $"Created crypto product ({crypto.ProductId}) in Lyra tenant ({tenant})");
                }
                catch (Lyra.Persistence.Exceptions.AlreadyExistsException ex)
                {
                    // suppress
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        $"Could not create crypto product ({crypto.ProductId}) in Lyra tenant ({tenant})");
                }
            }
        }

        private async Task ProvisionCopyrightTokensWithInstruments(ProductWriteRepository repository, string tenant,
            List<(Instrument, CopyrightToken)> tokensAndCryptos)
        {
            foreach (var (instrument, token) in tokensAndCryptos)
            {
                try
                {
                    logger.LogInformation(
                        $"Creating copyright token ({token.ProductId}) in Lyra tenant ({tenant})");
                    await repository.AddCopyrightTokenWithInstrumentAsync(tenant, token, instrument);
                    logger.LogDebug(
                        $"Created copyright token ({token.ProductId}) in Lyra tenant ({tenant})");
                }
                catch (Lyra.Persistence.Exceptions.AlreadyExistsException ex)
                {
                    // suppress
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex,
                        $"Could not create copyright token ({token.ProductId}) in Lyra tenant ({tenant})");
                }
            }
        }

        private async Task ProvisionCrypto(ProductWriteRepository repository, string tenant,
            Instrument[] cryptoInstruments)
        {
            foreach (var crypto in cryptoInstruments)
            {
                try
                {
                    logger.LogInformation($"Creating crypto instrument ({crypto.Id}) in Lyra tenant ({tenant})");
                    await repository.AddInstrumentAsync(tenant, crypto);
                    logger.LogDebug($"Created crypto instrument ({crypto.Id}) in Lyra tenant ({tenant})");
                }
                catch (Lyra.Persistence.Exceptions.AlreadyExistsException ex)
                {
                    // suppress
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        $"Could not create crypto instrument ({crypto.Id}) in Lyra tenant ({tenant})");
                }
            }
        }

        private async Task ProvisionSimple(ProductWriteRepository repository, string tenant,
            Instrument[] simpleInstruments)
        {
            foreach (var simple in simpleInstruments)
            {
                try
                {
                    logger.LogInformation($"Creating simple instrument ({simple.Id}) in Lyra tenant ({tenant})");
                    await repository.AddInstrumentAsync(tenant, simple);
                    logger.LogDebug($"Created simple instrument ({simple.Id}) in Lyra tenant ({tenant})");
                }
                catch (Lyra.Persistence.Exceptions.AlreadyExistsException ex)
                {
                    // suppress
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        $"Could not create simple instrument ({simple.Id}) in Lyra tenant ({tenant})");
                }
            }
        }

        private async Task ProvisionFiat(ProductWriteRepository repository, string tenant, Instrument[] fiatInstruments)
        {
            foreach (var fiat in fiatInstruments)
            {
                try
                {
                    logger.LogInformation($"Creating fiat instrument ({fiat.Id}) in Lyra tenant ({tenant})");
                    await repository.AddInstrumentAsync(tenant, fiat);
                    logger.LogDebug($"Created fiat instrument ({fiat.Id}) in Lyra tenant ({tenant})");
                }
                catch (Lyra.Persistence.Exceptions.AlreadyExistsException ex)
                {
                    // suppress
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Could not create fiat instrument ({fiat.Id}) in Lyra tenant ({tenant})");
                }
            }
        }

        /// <summary>
        /// Removing Copyright Products with instruments.
        /// </summary>
        /// <param name="tenants">List of Tenants </param>
        /// <returns>Delete the Products and Instruments for the Provided Tenant</returns>
        private async Task RemoveCopyrightTokensWithInstruments(List<string> tenants)
        {
            var productWriteRepository = new ProductWriteRepository(client, NullLogger<ProductWriteRepository>.Instance);
            var productReadRepository = new ProductReadRepository(client, NullLogger<ProductReadRepository>.Instance);
            var result = new ListResult<Product>();
            var productids = new List<string>();
            foreach (var tenant in tenants)
            {
                do
                {
                    // Get all the Paginated Copy right Products
                    result = await productReadRepository.GetAllProductsPaginatedAsync(tenant, ProductTypes.CopyrightToken, 40, result.Next);
                    if (result != null || result.Items.Count != 0)
                    {
                        productids.AddRange(result.Items.Select(x => x.ProductId));
                    }
                } while (result.HasNext);

                foreach (var id in productids)
                {
                    try
                    {
                        await productWriteRepository.RemoveProductWithInstrumentAsync(tenant, id, ProductTypes.CopyrightToken);
                        logger.LogInformation("Deleted copyright token product ({symbol}) in Lyra tenant ({tenant})", id, tenant);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("Exception ocurred while copyright product deletion for Tenant : {Tenant} and Exception is: {Exception} with product Id {ProductId}", tenant, ex, id);
                    }
                }
            }
        }

        private async Task RemoveCryptosWithInstruments(ProductWriteRepository repository, string tenant, string[] cryptoIds)
        {
            foreach (var id in cryptoIds)
            {
                logger.LogInformation("Deleting crypto ({symbol}) in Lyra tenant ({tenant})", id, tenant);
                await repository.RemoveProductWithInstrumentAsync(tenant, id, ProductTypes.Crypto);
                logger.LogDebug("Deleted crypto ({symbol}) in Lyra tenant ({tenant})", id, tenant);
            }
        }

        private async Task RemoveShareTokensWithInstruments(ProductWriteRepository repository, string tenant, string[] shareTokenIds)
        {
            foreach (var id in shareTokenIds)
            {
                logger.LogInformation("Deleting share token ({symbol}) in Lyra tenant ({tenant})", id, tenant);
                await repository.RemoveProductWithInstrumentAsync(tenant, id, ProductTypes.ShareToken);
                logger.LogDebug("Deleted share token ({symbol}) in Lyra tenant ({tenant})", id, tenant);
            }
        }

        private async Task RemoveInstruments(ProductWriteRepository repository, string tenant, string[] instrumentIds)
        {
            foreach (var id in instrumentIds)
            {
                logger.LogInformation("Deleting instrument ({symbol}) in Lyra tenant ({tenant})", id, tenant);
                await repository.RemoveInstrumentAsync(tenant, id);
                logger.LogDebug("Deleted instrument ({symbol}) in Lyra tenant ({tenant})", id, tenant);
            }
        }

        public async Task SetTableThroughputOnDemand(string tableName)
        {
            var request = new UpdateTableRequest
            {
                TableName = tableName,
                BillingMode = BillingMode.PAY_PER_REQUEST
            };

            await this.client.UpdateTableAsync(request);
        }

        public async Task<string> CreateTable(string tableName, ILogger logger, CancellationToken cancellationToken)
        {
            if (await Exists(tableName, cancellationToken))
            {
                if (logger != null)
                {
                    logger.LogInformation($"Table '{tableName}' already exists, skipping creation");
                }
                else
                {
                    Console.WriteLine($"Table '{tableName}' already exists, skipping creation");
                }

                return tableName;
            }

            if (logger != null)
            {
                logger.LogInformation($"Creating table '{tableName}'...");
            }
            else
            {
                Console.WriteLine($"Creating table '{tableName}'...");
            }

            await client.CreateTableAsync(
                new CreateTableRequest
                {
                    TableName = tableName,
                    ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                    KeySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement { AttributeName = Schema.Attributes.PartitionKey, KeyType = KeyType.HASH },
                        new KeySchemaElement { AttributeName = Schema.Attributes.SortKey, KeyType = KeyType.RANGE },
                    },
                    AttributeDefinitions = GetCurrentAttributeDefinitionsForTable(),
                    GlobalSecondaryIndexes = new List<GlobalSecondaryIndex>
                    {
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.CopyrightTokensByCreatorId,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.CopyrightToken.Attributes.CreatorIdPartitionKey,
                                    KeyType = KeyType.HASH
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.CopyrightToken.Attributes.CreatorIdSortKey,
                                    KeyType = KeyType.RANGE
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                                { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        },
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.CopyrightTokensByExternalMusicId,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.CopyrightToken.Attributes.ExternalMusicIdPartitionKey,
                                    KeyType = KeyType.HASH
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.CopyrightToken.Attributes.ExternalMusicIdSortKey,
                                    KeyType = KeyType.RANGE
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                                { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        },
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.CopyrightTokensBySecondaryMarketAvailability,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.CopyrightToken.Attributes
                                        .SecondaryMarketAvailabilityPartitionKey,
                                    KeyType = KeyType.HASH
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilitySortKey,
                                    KeyType = KeyType.RANGE
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                                { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        },
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.CopyrightTokensByTradingVolume,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement { AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey, KeyType = KeyType.HASH },
                                new KeySchemaElement { AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityTradingVolumeSortKey, KeyType = KeyType.RANGE }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        },
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.Products,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Product.CommonAttributes.ProductPartitionKey,
                                    KeyType = KeyType.HASH
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Product.CommonAttributes.ProductSortKey,
                                    KeyType = KeyType.RANGE
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                                { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        },
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.Instruments,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Instrument.Attributes.InstrumentTenantPartitionKey,
                                    KeyType = KeyType.HASH
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Instrument.Attributes.InstrumentTenantSortKey,
                                    KeyType = KeyType.RANGE
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                                { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        },
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.ProductStatus,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Product.CommonAttributes.ProductStatusPartitionKey,
                                    KeyType = KeyType.HASH
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Product.CommonAttributes.ProductStatusSortKey,
                                    KeyType = KeyType.RANGE
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                                { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        },
                        new GlobalSecondaryIndex
                        {
                            IndexName = Table.Indicies.InstrumentStatus,
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Instrument.Attributes.InstrumentStatusPartitionKey,
                                    KeyType = KeyType.HASH
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = Schema.Instrument.Attributes.InstrumentStatusSortKey,
                                    KeyType = KeyType.RANGE
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                                { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                            Projection = new Projection { ProjectionType = ProjectionType.ALL },
                        }
                    }
                },
                cancellationToken);

            // TODO (Cameron): Handle cancellation and timeout.
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));

                var tableExists = await Exists(tableName, cancellationToken);
                if (tableExists)
                {
                    return tableName;
                }
            }
        }

        public async Task RemoveOldIndicies(string tableName)
        {
            var tableDescription = await this.client.DescribeTableAsync(new DescribeTableRequest
            {
                TableName = tableName
            });

            if (tableDescription.Table.GlobalSecondaryIndexes.Any(x => x.IndexName == "entity_type"))
            {
                await this.client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = GetAlittleBitOldAttributeDefinitionsForTable(),
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Delete = new DeleteGlobalSecondaryIndexAction() { IndexName = "entity_type" }
                        }
                    }
                });

                await RemoveAttribute(tableName, "entity_type");
                await RemoveAttribute(tableName, "entity_subtype");

            }

            if (tableDescription.Table.GlobalSecondaryIndexes.Any(x => x.IndexName == "products_by_category"))
            {

                await this.client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = GetCurrentAttributeDefinitionsForTable(),
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Delete = new DeleteGlobalSecondaryIndexAction() { IndexName = "products_by_category", }
                        }
                    }
                });

                await RemoveAttribute(tableName, "product.category_partition_key");
                await RemoveAttribute(tableName, "product.category_sort_key");
            }
        }

        private async Task RemoveAttribute(string tableName, string name)
        {
            var orderEntitiesScan = await client.ScanAsync(
                new ScanRequest()
                {
                    TableName = tableName,
                    ConsistentRead = true,
                    FilterExpression = $"attribute_exists(#key)",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#key", $"{name}" },
                    }
                }, CancellationToken.None);

            foreach (var item in orderEntitiesScan.Items)
            {
                var partitionKeyValue = item.GetValueOrDefault("partition_key")?.S;
                var sortKeyValue = item.GetValueOrDefault("sort_key")?.S;

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            { "partition_key", new AttributeValue { S = partitionKeyValue } },
                            { "sort_key", new AttributeValue { S = sortKeyValue } }
                        },
                        UpdateExpression = $"REMOVE #key",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#key", $"{name}" },
                        },
                    });
            }
        }

        private static List<AttributeDefinition> GetAlittleBitOldAttributeDefinitionsForTable()
        {
            return new List<AttributeDefinition>
            {
                new AttributeDefinition
                    { AttributeName = Schema.Attributes.PartitionKey, AttributeType = ScalarAttributeType.S },
                new AttributeDefinition
                    { AttributeName = Schema.Attributes.SortKey, AttributeType = ScalarAttributeType.S },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.CreatorIdPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.CreatorIdSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.ExternalMusicIdPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.ExternalMusicIdSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilitySortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                    { AttributeName = "product.category_partition_key", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition
                    { AttributeName = "product.category_sort_key", AttributeType = ScalarAttributeType.S },
                new AttributeDefinition
                {
                    AttributeName = Schema.Product.CommonAttributes.ProductPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.Product.CommonAttributes.ProductSortKey,
                    AttributeType = ScalarAttributeType.S
                }
            };
        }

        private static List<AttributeDefinition> GetCurrentAttributeDefinitionsForTable()
        {
            return new List<AttributeDefinition>
            {
                new AttributeDefinition
                    { AttributeName = Schema.Attributes.PartitionKey, AttributeType = ScalarAttributeType.S },
                new AttributeDefinition
                    { AttributeName = Schema.Attributes.SortKey, AttributeType = ScalarAttributeType.S },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.CreatorIdPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.CreatorIdSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.ExternalMusicIdPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.ExternalMusicIdSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilitySortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityTradingVolumeSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.Product.CommonAttributes.ProductPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.Product.CommonAttributes.ProductSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.Instrument.Attributes.InstrumentTenantPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.Instrument.Attributes.InstrumentTenantSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                  new AttributeDefinition
                {
                    AttributeName = Schema.Instrument.Attributes.InstrumentStatusPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.Instrument.Attributes.InstrumentStatusSortKey,
                    AttributeType = ScalarAttributeType.S
                },
                 new AttributeDefinition
                {
                    AttributeName = Schema.Product.CommonAttributes.ProductStatusPartitionKey,
                    AttributeType = ScalarAttributeType.S
                },
                new AttributeDefinition
                {
                    AttributeName = Schema.Product.CommonAttributes.ProductStatusSortKey,
                    AttributeType = ScalarAttributeType.S
                }
            };
        }

        public async Task<bool> Exists(string tableName, CancellationToken cancellationToken)
        {
            var response = await this.client.ListTablesAsync(cancellationToken);
            return response.TableNames.Contains(tableName);
        }

        public async Task<List<(CopyrightToken, OldSongModel)>> GetProductsUnmigratedContributors(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();

            Dictionary<string, AttributeValue> lastKeyEvaluated = null;
            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                            { "#songDetails", Schema.CopyrightToken.Attributes.SongDetails },
                            { "#category", Schema.Product.CommonAttributes.Category }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"PRODUCT") },
                            { ":contributors", new AttributeValue($"Contributors") },
                            { ":category", new AttributeValue(ProductTypes.CopyrightToken) }
                        },
                        FilterExpression =
                            "contains(#partitionKey, :partitionKeyChunk) AND NOT contains(#songDetails, :contributors) AND #category=:category"
                    });

                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return result.Select(MapCopyrightToken).ToList();
        }

        public async Task FillTradingVolume(string tableName)
        {
            logger.LogInformation("Starting provisioning of trading volume");
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;
            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#trading_volume", Schema.CopyrightToken.Attributes.TradingVolume },
                            { "#category", Schema.Product.CommonAttributes.Category }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {":category", new AttributeValue("CopyrightToken")}
                        },
                        FilterExpression = "attribute_not_exists(#trading_volume) and #category=:category",
                    });

                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);


            logger.LogInformation($"Executing provisioning of trading volume for {result.Count} items.");

            foreach (var item in result)
            {
                var partitionKeyValue = item.GetValueOrDefault("partition_key")?.S;
                var sortKeyValue = item.GetValueOrDefault("sort_key")?.S;

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            { "partition_key", new AttributeValue { S = partitionKeyValue } },
                            { "sort_key", new AttributeValue { S = sortKeyValue } }
                        },
                        UpdateExpression =
                            "set #copyright_token_trading_volume = :copyright_token_trading_volume, " +
                            "#copyright_token_secondary_market_availability_trading_volume_sort_key = :copyright_token_secondary_market_availability_trading_volume_sort_key",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#copyright_token_trading_volume", Schema.CopyrightToken.Attributes.TradingVolume },
                            { "#copyright_token_secondary_market_availability_trading_volume_sort_key", Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityTradingVolumeSortKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":copyright_token_trading_volume", new AttributeValue { N = "0" } },
                            { ":copyright_token_secondary_market_availability_trading_volume_sort_key", new AttributeValue { S = Schema.CopyrightToken.GetSecondaryMarketAvailabilityTradingVolumeSortKey(0) } },
                        }
                    });
            }

            logger.LogDebug($"Provisioning of trading volume completed.");
        }

        private async Task CreateCopyrightTokensByTradingVolumeIfNotExist(string tableName)
        {
            logger.LogInformation("Checking for CopyrightTokensByTradingVolume existence...");
            var tableDescription = await client.DescribeTableAsync(new DescribeTableRequest { TableName = tableName });
            var globalSecondaryIndexes = tableDescription.Table.GlobalSecondaryIndexes;
            if (!globalSecondaryIndexes.Exists(x => x.IndexName == Table.Indicies.CopyrightTokensByTradingVolume))
            {
                logger.LogInformation("Creating CopyrightTokensByTradingVolume index started.");
                await this.client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = GetCurrentAttributeDefinitionsForTable(),
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Create = new CreateGlobalSecondaryIndexAction
                            {
                                IndexName = Table.Indicies.CopyrightTokensByTradingVolume,
                                KeySchema = new List<KeySchemaElement>
                                {
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityPartitionKey,
                                        KeyType = KeyType.HASH
                                    },
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.CopyrightToken.Attributes.SecondaryMarketAvailabilityTradingVolumeSortKey,
                                        KeyType = KeyType.RANGE
                                    }
                                },
                                Projection = new Projection { ProjectionType = ProjectionType.ALL },
                            }
                        },
                    }
                });
                logger.LogDebug("Creating CopyrightTokensByTradingVolume index completed.");
            }
            else
            {
                logger.LogInformation("Creating CopyrightTokensByTradingVolume skipped: index already exists.");
            }
        }

        public async Task MigrateMintedField(string tableName)
        {
            var orderEntitiesScan = await client.ScanAsync(
                new ScanRequest()
                {
                    TableName = tableName,
                    ConsistentRead = true,
                    FilterExpression =
                        "attribute_exists(#copyright_token_is_minted)",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        {
                            "#copyright_token_is_minted",
                            "copyright_token.is_minted"
                        }
                    }
                }, CancellationToken.None);

            foreach (var item in orderEntitiesScan.Items)
            {
                var partitionKeyValue = item.GetValueOrDefault("partition_key")?.S;
                var sortKeyValue = item.GetValueOrDefault("sort_key")?.S;
                if (!bool.TryParse(item.GetValueOrDefault("copyright_token.is_minted")?.S,
                        out var copyrightTokenIsMintedValue))
                {
                    continue;
                }

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                "partition_key",
                                new AttributeValue
                                    { S = partitionKeyValue }
                            },
                            {
                                "sort_key",
                                new AttributeValue { S = sortKeyValue }
                            }
                        },
                        UpdateExpression =
                            "set #product_is_minted = :copyright_token_is_minted_value",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            {
                                "#product_is_minted",
                                "product.is_minted"
                            },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":copyright_token_is_minted_value",
                                new AttributeValue
                                {
                                    BOOL = copyrightTokenIsMintedValue
                                }
                            },
                        }
                    });

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                "partition_key",
                                new AttributeValue
                                    { S = partitionKeyValue }
                            },
                            {
                                "sort_key",
                                new AttributeValue { S = sortKeyValue }
                            }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                "copyright_token.is_minted",
                                new AttributeValueUpdate()
                                {
                                    Action = AttributeAction.DELETE
                                }
                            }
                        }
                    });
            }
        }

        public async Task MigrateContributors(string tableName)
        {
            var unmigratedCopyrightTokens = await this.GetProductsUnmigratedContributors(tableName);

            foreach (var (copyrightToken, songDetails) in unmigratedCopyrightTokens)
            {
                var contributors = new Contributors();

                if (!string.IsNullOrWhiteSpace(songDetails.Owner))
                    contributors.Owners.Add(new Contributor(null, null, songDetails.Owner, 0m));
                if (!string.IsNullOrWhiteSpace(songDetails.Composer))
                    contributors.Composers.Add(new Contributor(null, null, songDetails.Composer, 0m));
                if (!string.IsNullOrWhiteSpace(songDetails.Engineer))
                    contributors.Engineers.Add(new Contributor(null, null, songDetails.Engineer, 0m));
                if (!string.IsNullOrWhiteSpace(songDetails.Lyricist))
                    contributors.Lyricists.Add(new Contributor(null, null, songDetails.Lyricist, 0m));
                if (!string.IsNullOrWhiteSpace(songDetails.Producer))
                    contributors.Producers.Add(new Contributor(null, null, songDetails.Producer, 0m));
                if (!string.IsNullOrWhiteSpace(songDetails.Songwriter))
                    contributors.Songwriters.Add(new Contributor(null, null, songDetails.Songwriter, 0m));
                if (!string.IsNullOrWhiteSpace(songDetails.FeaturedArtist))
                    contributors.FeaturedArtists.Add(new Contributor(null, null, songDetails.FeaturedArtist, 0m));
                if (!string.IsNullOrWhiteSpace(songDetails.NonFeaturedMusician))
                    contributors.NonFeaturedMusicians.Add(new Contributor(null, null, songDetails.NonFeaturedMusician,
                        0m));
                if (!string.IsNullOrWhiteSpace(songDetails.NonFeaturedVocalist))
                    contributors.NonFeaturedVocalists.Add(new Contributor(null, null, songDetails.NonFeaturedVocalist,
                        0m));

                copyrightToken.AddContributors(contributors);

                await this.client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue
                                {
                                    S = Schema.Product.GetPrimaryPartitionKey(copyrightToken.TenantId,
                                        copyrightToken.ProductId)
                                }
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue { S = Schema.Product.GetPrimarySortKey(copyrightToken.Category) }
                            }
                        },
                        UpdateExpression = "set #song_details=:song_details",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#song_details", Schema.CopyrightToken.Attributes.SongDetails },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":song_details",
                                new AttributeValue(JsonConvert.SerializeObject(copyrightToken.SongDetails))
                            }
                        }
                    });
            }
        }

        public async Task<IEnumerable<Dictionary<string, AttributeValue>>>
          GetAllProductsAsync(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"PRODUCT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return result;
        }


        public async Task<IEnumerable<Dictionary<string, AttributeValue>>>
            GetAllProductsWithoutProductsIndexFieldsAsync(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"PRODUCT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                result.AddRange(response.Items.Where(x =>
                    !x.Keys.Contains(Schema.Product.CommonAttributes.ProductPartitionKey) ||
                    !x.Keys.Contains(Schema.Product.CommonAttributes.ProductSortKey)));

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return result;
        }

        public async Task<IEnumerable<Dictionary<string, AttributeValue>>>
          GetAllProductsWithRespectToProductStatus(string tableName, bool getOnlyThoseWhoHasProductStatus = false)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"PRODUCT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                if (getOnlyThoseWhoHasProductStatus)
                {
                    result.AddRange(response.Items.Where(x =>
                        x.Keys.Contains(Schema.Product.CommonAttributes.ProductStatusPartitionKey) ||
                        x.Keys.Contains(Schema.Product.CommonAttributes.ProductStatusSortKey)));

                }
                else 
                {
                    result.AddRange(response.Items.Where(x =>
                        !x.Keys.Contains(Schema.Product.CommonAttributes.ProductStatusPartitionKey) ||
                        !x.Keys.Contains(Schema.Product.CommonAttributes.ProductStatusSortKey)));
                }

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return result;
        }

        public async Task PopulateInstrumentIdForExistingProducts(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"PRODUCT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);
        }
        public async Task<IEnumerable<Dictionary<string, AttributeValue>>> GetAllInstrumentsWithoutIndex(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"INSTRUMENT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                result.AddRange(response.Items.Where(x =>
                    !x.Keys.Contains(Schema.Instrument.Attributes.InstrumentTenantPartitionKey) ||
                    !x.Keys.Contains(Schema.Instrument.Attributes.InstrumentTenantSortKey)));

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return result;
        }

        public async Task<IEnumerable<Dictionary<string, AttributeValue>>> GetAllInstrumentStatusWithoutIndex(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"INSTRUMENT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                result.AddRange(response.Items.Where(x =>
                    !x.Keys.Contains(Schema.Instrument.Attributes.InstrumentStatusPartitionKey) ||
                    !x.Keys.Contains(Schema.Instrument.Attributes.InstrumentStatusSortKey)));

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            return result;
        }

        private static Instrument MapInstrument(Dictionary<string, AttributeValue> item)
        {
            item.TryGetValue(Schema.Instrument.Attributes.NumberOfDecimalPlaces, out var numberOfDecimalPlacesAttrVal);
            item.TryGetValue(Schema.Instrument.Attributes.InstrumentStatus, out var instrumentStatusAttribute);

            return Instrument.RestoreFromDatabase(item[Schema.Instrument.Attributes.Id].S,
                item[Schema.Instrument.Attributes.Name].S,
                int.TryParse(numberOfDecimalPlacesAttrVal?.N, out var numberOfDecimalPlaces)
                    ? numberOfDecimalPlaces
                    : 0, int.TryParse(instrumentStatusAttribute?.N, out var InstStatsu) ? InstStatsu : (int)InstrumentStatus.Enabled);
        }

        private Product MapProduct(Dictionary<string, AttributeValue> item)
        {
            try
            {
                _ = item.TryGetValue(Schema.Product.CommonAttributes.Category, out var productCategory);
                var category = !string.IsNullOrEmpty(productCategory?.S) ? productCategory.S : string.Empty;

                if (!string.IsNullOrEmpty(category))
                {
                    var cultureInfo = CultureInfo.InvariantCulture;
                    var ProductStatus = (int)ProductsStatus.Enabled;
                    item.TryGetValue(Schema.Product.CommonAttributes.Id, out var productID);
                    item.TryGetValue(Schema.ShareToken.Attributes.TotalSupply, out var totalSupplyAttrVal);
                    item.TryGetValue(Schema.Product.CommonAttributes.ProductStatus, out var productStatusAttribute);
                    item.TryGetValue(Schema.Product.CommonAttributes.InstrumentId, out var productInstrumentIdAttribute);
                    item.TryGetValue(Schema.ShareToken.Attributes.ExternalAssetId, out var shareTokenExternalAssetIdAttribute);
                    item.TryGetValue(Schema.Crypto.Attributes.ExternalAssetId, out var cryptoExternalAssetIdAttribute);

                    switch (category.Trim().ToUpper())
                    {
                        case "CRYPTO":
                            return Crypto.RestoreFromDatabase(
                                item[Schema.Product.CommonAttributes.Id].S,
                                item[Schema.Product.CommonAttributes.Color].S,
                                item.TryGetValue(Schema.Product.CommonAttributes.IsMinted, out var cr) && cr.BOOL,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productID.S,
                                int.TryParse(productStatusAttribute?.N, out var c) ? c : ProductStatus,
                                !string.IsNullOrEmpty(cryptoExternalAssetIdAttribute?.S) ? cryptoExternalAssetIdAttribute.S : string.Empty);

                        case "SIMPLE":
                            return Simple.RestoreFromDatabase(
                                item[Schema.Product.CommonAttributes.Id].S,
                                item[Schema.Product.CommonAttributes.Color].S,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productID.S,
                                int.TryParse(productStatusAttribute?.N, out var s) ? s : ProductStatus);

                        case "FIAT":
                            return Fiat.RestoreFromDatabase(
                                item[Schema.Product.CommonAttributes.Id].S,
                                item[Schema.Product.CommonAttributes.Color].S,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productID.S,
                                int.TryParse(productStatusAttribute?.N, out var f) ? f : ProductStatus);

                        case "COPYRIGHTTOKEN":
                            return CopyrightToken.RestoreFromDatabase(
                                item[Schema.Product.CommonAttributes.Id].S,
                                item[Schema.CopyrightToken.Attributes.ExternalMusicId].S,
                                item[Schema.CopyrightToken.Attributes.CreatorId].S,
                                item[Schema.CopyrightToken.Attributes.Icon].S,
                                item[Schema.Product.CommonAttributes.Color].S,
                                item[Schema.Attributes.PartitionKey].S.Split('#').ToArray()[1],
                                item[Schema.CopyrightToken.Attributes.SubType].S,
                                item[Schema.CopyrightToken.Attributes.Ownership].S,
                                decimal.Parse(item[Schema.CopyrightToken.Attributes.Amount].S, cultureInfo),
                                item[Schema.CopyrightToken.Attributes.SongDetails].S,
                                decimal.Parse(item[Schema.CopyrightToken.Attributes.AlreadyAuctionedAmount].S,
                                    CultureInfo.InvariantCulture),
                                bool.Parse(item[Schema.CopyrightToken.Attributes.IsAvailableForSecondaryMarket].S),
                                item.TryGetValue(Schema.Product.CommonAttributes.IsMinted, out var a) && a.BOOL,
                                decimal.Parse(item[Schema.CopyrightToken.Attributes.TradingVolume].N, cultureInfo),
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : string.Empty,
                                int.TryParse(productStatusAttribute?.N, out var ct) ? ct : ProductStatus);
                        case "SHARETOKEN":
                            return ShareToken.RestoreFromDatabase(item[Schema.Product.CommonAttributes.Id].S,
                                item[Schema.ShareToken.Attributes.Name].S,
                                item[Schema.ShareToken.Attributes.Ticker].S,
                                item[Schema.ShareToken.Attributes.DocumentUrl].S,
                                item.TryGetValue(Schema.Product.CommonAttributes.IsMinted, out var isMinted) && isMinted.BOOL,
                                item[Schema.Product.CommonAttributes.Color].S,
                                item.TryGetValue(Schema.ShareToken.Attributes.IsDeployed, out var isDeployed) &&
                                isDeployed.BOOL,
                                item.TryGetValue(Schema.ShareToken.Attributes.IsFrozen, out var isFrozen) && isFrozen.BOOL,
                                item[Schema.ShareToken.Attributes.BlockchainErrorMessage].S,
                                decimal.TryParse(totalSupplyAttrVal?.N,
                                    NumberStyles.Any,
                                    CultureInfo.InvariantCulture,
                                    out var totalSupply)
                                    ? totalSupply
                                    : 0m,
                                !string.IsNullOrEmpty(productInstrumentIdAttribute?.S) ? productInstrumentIdAttribute.S : productID.S,
                                int.TryParse(productStatusAttribute?.N, out var st) ? st : ProductStatus,
                                !string.IsNullOrEmpty(shareTokenExternalAssetIdAttribute?.S) ? shareTokenExternalAssetIdAttribute.S : string.Empty);
                    }
                }

                return default;
            }
            catch (Exception ex)
            {
                var dynamoAttributes = JsonConvert.SerializeObject(item);
                logger.LogError("Exception Ocurred and the message is  {Exception} and the serialized message is {Attribute}", ex, dynamoAttributes);
                throw;
            }
        }

        public async Task MigrateProducts(string tableName)
        {
            var tableDescription = await this.client.DescribeTableAsync(new DescribeTableRequest
            {
                TableName = tableName
            });

            if (tableDescription.Table.GlobalSecondaryIndexes.All(x => x.IndexName != Table.Indicies.Products))
            {
                await this.client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = new List<AttributeDefinition>
                    {
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Product.CommonAttributes.ProductPartitionKey,
                            AttributeType = ScalarAttributeType.S
                        },
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Product.CommonAttributes.ProductSortKey,
                            AttributeType = ScalarAttributeType.S
                        }
                    },
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Create = new CreateGlobalSecondaryIndexAction
                            {
                                IndexName = Table.Indicies.Products,
                                KeySchema = new List<KeySchemaElement>
                                {
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Product.CommonAttributes.ProductPartitionKey,
                                        KeyType = KeyType.HASH
                                    },
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Product.CommonAttributes.ProductSortKey,
                                        KeyType = KeyType.RANGE
                                    }
                                },
                                Projection = new Projection { ProjectionType = ProjectionType.ALL }
                            }
                        }
                    }
                });
            }

            var allProducts = await this.GetAllProductsWithoutProductsIndexFieldsAsync(tableName);

            foreach (var productAttributeValues in allProducts)
            {
                try
                {
                    var product = MapProduct(productAttributeValues);
                    var tenant = productAttributeValues[Schema.Attributes.PartitionKey].S.Split('#')[1];

                    await client.UpdateItemAsync(
                        new UpdateItemRequest
                        {
                            TableName = tableName,
                            Key = new Dictionary<string, AttributeValue>()
                            {
                                {
                                    Schema.Attributes.PartitionKey,
                                    new AttributeValue
                                        { S = Schema.Product.GetPrimaryPartitionKey(tenant, product.ProductId) }
                                },
                                {
                                    Schema.Attributes.SortKey,
                                    new AttributeValue { S = Schema.Product.GetPrimarySortKey(product.Category) }
                                }
                            },
                            UpdateExpression =
                                "set #productPartitionKey=:productPartitionKey, #productSortKey=:productSortKey",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#productPartitionKey", Schema.Product.CommonAttributes.ProductPartitionKey },
                                { "#productSortKey", Schema.Product.CommonAttributes.ProductSortKey }
                            },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                {
                                    ":productPartitionKey",
                                    new AttributeValue(Schema.Product.GetProductPartitionKey(tenant))
                                },
                                {
                                    ":productSortKey",
                                    new AttributeValue(
                                        Schema.Product.GetProductSortKey(product.Category, product.ProductId))
                                }
                            }
                        });
                }
                catch (Exception ex)
                {
                    
                    Console.WriteLine($"Does not work for product {JsonConvert.SerializeObject(productAttributeValues)}");
                    throw;
                }
            }
        }

        public async Task MigrateProductStatusSortKey(string tableName)
        {
            var allProducts = await this.GetAllProductsAsync(tableName);

            foreach (var productAttributeValues in allProducts)
            {
                var product = MapProduct(productAttributeValues);
                var tenant = productAttributeValues[Schema.Attributes.PartitionKey].S.Split('#')[1];

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue { S = Schema.Product.GetPrimaryPartitionKey(tenant, product.ProductId) }
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue { S = Schema.Product.GetPrimarySortKey(product.Category) }
                            }
                       },
                        UpdateExpression =
                            "set #productStatusSortKey=:productStatusSortKey",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#productStatusSortKey", Schema.Product.CommonAttributes.ProductStatusSortKey }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":productStatusSortKey",
                                new AttributeValue(
                                    Schema.Product.GetProductStatusSortKey(product.Category, product.ProductId))
                            }
                        }
                    });
            }
        }

        /// <summary>
        /// This is for Provisioning the Product Status Field.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <returns></returns>
        private async Task ProvisionProductStatusIndexValues(string tableName)
        {
            //Set all to Default and Enabled
            int productStatus = (int)ProductsStatus.Enabled;

            var tableDescription = await this.client.DescribeTableAsync(new DescribeTableRequest
            {
                TableName = tableName
            });

            if (tableDescription.Table.GlobalSecondaryIndexes.All(x => x.IndexName != Table.Indicies.ProductStatus))
            {
                await this.client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = new List<AttributeDefinition>
                    {
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Product.CommonAttributes.ProductStatusPartitionKey,
                            AttributeType = ScalarAttributeType.S
                        },
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Product.CommonAttributes.ProductStatusSortKey,
                            AttributeType = ScalarAttributeType.S
                        }
                    },
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Create = new CreateGlobalSecondaryIndexAction
                            {
                                IndexName = Table.Indicies.ProductStatus,
                                KeySchema = new List<KeySchemaElement>
                                {
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Product.CommonAttributes.ProductStatusPartitionKey,
                                        KeyType = KeyType.HASH
                                    },
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Product.CommonAttributes.ProductStatusSortKey,
                                        KeyType = KeyType.RANGE
                                    }
                                },
                                Projection = new Projection { ProjectionType = ProjectionType.ALL }
                            }
                        }
                    }
                });
            }

            var allProducts = await this.GetAllProductsWithRespectToProductStatus(tableName);

            foreach (var productAttributeValues in allProducts)
            {
                var product = MapProduct(productAttributeValues);
                var tenant = productAttributeValues[Schema.Attributes.PartitionKey].S.Split('#')[1];

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue { S = Schema.Product.GetProductPartitionKey(tenant) }
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue { S = Schema.Product.GetProductSortKey(product.Category, product.ProductId) }
                            }
                        },
                        UpdateExpression =
                            "set #productStatusPartitionKey=:productStatusPartitionKey, #productStatusSortKey=:productStatusSortKey, #ProductStatus=:ProductStatus",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#productStatusPartitionKey", Schema.Product.CommonAttributes.ProductStatusPartitionKey },
                            { "#productStatusSortKey", Schema.Product.CommonAttributes.ProductStatusSortKey },
                            { "#ProductStatus", Schema.Product.CommonAttributes.ProductStatus },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":productStatusPartitionKey",
                                new AttributeValue(Schema.Product.GetProductStatusPartitionKey(tenant, productStatus))
                            },
                            {
                                ":productStatusSortKey",
                                new AttributeValue(Schema.Product.GetProductStatusSortKey(product.Category, product.ProductId))
                            },
                            {
                             ":ProductStatus",new AttributeValue() {N = productStatus.ToString() }
                            }
                        }
                    });
            }
        }
        private async Task ProvisionInstrumentsIndexValues(string tableName)
        {
            var tableDescription = await this.client.DescribeTableAsync(new DescribeTableRequest
            {
                TableName = tableName
            });

            if (tableDescription.Table.GlobalSecondaryIndexes.All(x => x.IndexName != Table.Indicies.Instruments))
            {
                await this.client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = new List<AttributeDefinition>
                    {
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Instrument.Attributes.InstrumentTenantPartitionKey,
                            AttributeType = ScalarAttributeType.S
                        },
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Instrument.Attributes.InstrumentTenantSortKey,
                            AttributeType = ScalarAttributeType.S
                        }
                    },
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Create = new CreateGlobalSecondaryIndexAction
                            {
                                IndexName = Table.Indicies.Instruments,
                                KeySchema = new List<KeySchemaElement>
                                {
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Instrument.Attributes.InstrumentTenantPartitionKey,
                                        KeyType = KeyType.HASH
                                    },
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Instrument.Attributes.InstrumentTenantSortKey,
                                        KeyType = KeyType.RANGE
                                    }
                                },
                                Projection = new Projection { ProjectionType = ProjectionType.ALL }
                            }
                        }
                    }
                });
            }

            var allInstruments = await this.GetAllInstrumentsWithoutIndex(tableName);

            foreach (var instrumentValues in allInstruments)
            {
                var instrument = MapInstrument(instrumentValues);
                var tenant = instrumentValues[Schema.Attributes.PartitionKey].S.Split('#')[1];

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue { S = Schema.Instrument.GetPartitionKey(tenant, instrument.Id) }
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue { S = Schema.Instrument.GetSortKey() }
                            }
                        },
                        UpdateExpression =
                            "set #instrumentTenantPartitionKey=:instrumentTenantPartitionKey, #instrumentTenantSortKey=:instrumentTenantSortKey",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#instrumentTenantPartitionKey", Schema.Instrument.Attributes.InstrumentTenantPartitionKey },
                            { "#instrumentTenantSortKey", Schema.Instrument.Attributes.InstrumentTenantSortKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":instrumentTenantPartitionKey",
                                new AttributeValue(Schema.Instrument.GetTenantPartitionKey(tenant))
                            },
                            {
                                ":instrumentTenantSortKey",
                                new AttributeValue(Schema.Instrument.GetTenantSortKey(instrument.Id))
                            },
                        }
                    });
            }
        }

        /// <summary>
        /// This Is for Provisioning the Instrument STatus Field.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <returns></returns>
        private async Task ProvisionInstrumentStatusIndexValues(string tableName)
        {
            //Set all to Default and Enabled
            int instrumentStatus = (int)InstrumentStatus.Enabled;

            var tableDescription = await this.client.DescribeTableAsync(new DescribeTableRequest
            {
                TableName = tableName
            });

            if (tableDescription.Table.GlobalSecondaryIndexes.All(x => x.IndexName != Table.Indicies.InstrumentStatus))
            {
                await this.client.UpdateTableAsync(new UpdateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = new List<AttributeDefinition>
                    {
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Instrument.Attributes.InstrumentStatusPartitionKey,
                            AttributeType = ScalarAttributeType.S
                        },
                        new AttributeDefinition
                        {
                            AttributeName = Schema.Instrument.Attributes.InstrumentStatusSortKey,
                            AttributeType = ScalarAttributeType.S
                        }
                    },
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Create = new CreateGlobalSecondaryIndexAction
                            {
                                IndexName = Table.Indicies.InstrumentStatus,
                                KeySchema = new List<KeySchemaElement>
                                {
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Instrument.Attributes.InstrumentStatusPartitionKey,
                                        KeyType = KeyType.HASH
                                    },
                                    new KeySchemaElement
                                    {
                                        AttributeName = Schema.Instrument.Attributes.InstrumentStatusSortKey,
                                        KeyType = KeyType.RANGE
                                    }
                                },
                                Projection = new Projection { ProjectionType = ProjectionType.ALL }
                            }
                        }
                    }
                });
            }

            var allInstruments = await this.GetAllInstrumentStatusWithoutIndex(tableName);

            foreach (var instrumentValues in allInstruments)
            {
                var instrument = MapInstrument(instrumentValues);
                var tenant = instrumentValues[Schema.Attributes.PartitionKey].S.Split('#')[1];

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue { S = Schema.Instrument.GetPartitionKey(tenant, instrument.Id) }
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue { S = Schema.Instrument.GetSortKey() }
                            }
                        },
                        UpdateExpression =
                            "set #instrumentStatusPartitionKey=:instrumentStatusPartitionKey, #instrumentStatusSortKey=:instrumentStatusSortKey, #InstrumentStatus=:InstrumentStatus",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#instrumentStatusPartitionKey", Schema.Instrument.Attributes.InstrumentStatusPartitionKey },
                            { "#instrumentStatusSortKey", Schema.Instrument.Attributes.InstrumentStatusSortKey },
                            { "#InstrumentStatus", Schema.Instrument.Attributes.InstrumentStatus }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":instrumentStatusPartitionKey",
                                new AttributeValue(Schema.Instrument.GetInstrumentStatusPartitionKey(tenant, instrumentStatus))
                            },
                            {
                                ":instrumentStatusSortKey",
                                new AttributeValue(Schema.Instrument.GetInstrumentStatusSortKey(instrument.Id))
                            },
                            {
                                ":InstrumentStatus",
                                new AttributeValue() {N = instrumentStatus.ToString()}
                            }
                        }
                    });
            }
        }

        /// <summary>
        /// This is for provisioning the product category Field.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <returns></returns>
        public async Task ProvisionProductCategoryValues(string tableName)
        {
            try
            {
                // Get all the Product with Product Status
                var allProducts = await this.GetAllProductsWithRespectToProductStatus(tableName, true);

                foreach (var productAttributeValues in allProducts)
                {
                    var product = MapProduct(productAttributeValues);
                    var tenant = productAttributeValues[Schema.Attributes.PartitionKey].S.Split('#')[1];

                    await client.UpdateItemAsync(
                        new UpdateItemRequest
                        {
                            TableName = tableName,
                            Key = new Dictionary<string, AttributeValue>()
                            {
                                {
                                    Schema.Attributes.PartitionKey,
                                    new AttributeValue { S = Schema.Product.GetProductPartitionKey(tenant) }
                                },
                                {
                                    Schema.Attributes.SortKey,
                                    new AttributeValue
                                        { S = Schema.Product.GetProductSortKey(product.Category, product.ProductId) }
                                }
                            },
                            UpdateExpression = "set #ProductCategory=:ProductCategory",
                            ExpressionAttributeNames = new Dictionary<string, string>
                            {
                                { "#ProductCategory", Schema.Product.CommonAttributes.Category },
                            },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":ProductCategory", new AttributeValue() { S = product.Category } }
                            }
                        });
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Provisioning for Product category failed");
            }
        }
        public async Task RemoveNumberOfDecimalPlacesFieldFromProductItems(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"PRODUCT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            foreach (var productAttributeValues in result)
            {
                var product = MapProduct(productAttributeValues);
                var tenant = productAttributeValues[Schema.Attributes.PartitionKey].S.Split('#')[1];

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue
                                    { S = Schema.Product.GetPrimaryPartitionKey(tenant, product.ProductId) }
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue { S = Schema.Product.GetPrimarySortKey(product.Category) }
                            }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                 "product.number_of_decimal_places",
                                new AttributeValueUpdate()
                                {
                                    Action = AttributeAction.DELETE
                                }
                            }
                        }
                    });
            }
        }

        public async Task PopulateNumberOfDecimalPlacesForExistingInstruments(string tableName)
        {
            var result = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue> lastKeyEvaluated = null;

            do
            {
                var response = await this.client.ScanAsync(
                    new ScanRequest
                    {
                        TableName = tableName,
                        Limit = 10,
                        ExclusiveStartKey = lastKeyEvaluated,
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            { "#partitionKey", Schema.Attributes.PartitionKey },
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            { ":partitionKeyChunk", new AttributeValue($"INSTRUMENT") }
                        },
                        FilterExpression = "contains(#partitionKey, :partitionKeyChunk)"
                    });

                result.AddRange(response.Items);

                lastKeyEvaluated = response.LastEvaluatedKey;
            } while (lastKeyEvaluated != null && lastKeyEvaluated.Count != 0);

            foreach (var instrumentAttributeValues in result)
            {
                var instrument = MapInstrument(instrumentAttributeValues);
                var tenant = instrumentAttributeValues[Schema.Attributes.PartitionKey].S.Split('#')[1];
                int numberOfDecimalPlaces = 0;

                if (instrument.Id.Contains("currency.fiat") || instrument.Id.Contains("currency.simple")
                    || instrument.Id.Contains("USDT") || instrument.Id.Contains("USDC"))
                {
                    numberOfDecimalPlaces = 2;
                }
                else if (instrument.Id.Contains("DGD") || instrument.Id.Contains("CTC"))
                {
                    numberOfDecimalPlaces = 4;
                }
                else
                {
                    numberOfDecimalPlaces = 6;
                }

                await client.UpdateItemAsync(
                    new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {
                                Schema.Attributes.PartitionKey,
                                new AttributeValue
                                    { S = Schema.Instrument.GetPartitionKey(tenant, instrument.Id) }
                            },
                            {
                                Schema.Attributes.SortKey,
                                new AttributeValue { S = Schema.Instrument.GetSortKey() }
                            }
                        },
                        UpdateExpression =
                            "set #numberOfDecimalPlaces=:numberOfDecimalPlaces",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#numberOfDecimalPlaces", Schema.Instrument.Attributes.NumberOfDecimalPlaces }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {
                                ":numberOfDecimalPlaces",
                                new AttributeValue() {N = numberOfDecimalPlaces.ToString()}
                            }
                        }
                    });
            }
        }

        private static (CopyrightToken, OldSongModel) MapCopyrightToken(Dictionary<string, AttributeValue> item)
        {
            item.TryGetValue(Schema.Product.CommonAttributes.ProductStatus, out var productStatusAttribute);
            item.TryGetValue(Schema.Product.CommonAttributes.InstrumentId, out var productInstrumentIdAttribute);

            return (CopyrightToken.RestoreFromDatabase(
                    item[Schema.Product.CommonAttributes.Id].S,
                    item[Schema.CopyrightToken.Attributes.ExternalMusicId].S,
                    item[Schema.CopyrightToken.Attributes.CreatorId].S,
                    item[Schema.CopyrightToken.Attributes.Icon].S,
                    item[Schema.Product.CommonAttributes.Color].S,
                    item[Schema.Attributes.PartitionKey].S.Split('#').ToArray()[1],
                    item[Schema.CopyrightToken.Attributes.SubType].S,
                    item[Schema.CopyrightToken.Attributes.Ownership].S,
                    decimal.Parse(item[Schema.CopyrightToken.Attributes.Amount].S, CultureInfo.InvariantCulture),
                    item[Schema.CopyrightToken.Attributes.SongDetails].S,
                    decimal.Parse(item[Schema.CopyrightToken.Attributes.AlreadyAuctionedAmount].S,
                        CultureInfo.InvariantCulture),
                    bool.Parse(item[Schema.CopyrightToken.Attributes.IsAvailableForSecondaryMarket].S),
                    item[Schema.Product.CommonAttributes.IsMinted].BOOL,
                    decimal.Parse(item[Schema.CopyrightToken.Attributes.TradingVolume].N, CultureInfo.InvariantCulture),
                    !string.IsNullOrEmpty(productInstrumentIdAttribute.S) ? productInstrumentIdAttribute.S : string.Empty,
                    int.TryParse(productStatusAttribute?.N, out var productStatus) ? productStatus : 0),
                    JsonConvert.DeserializeObject<OldSongModel>(item[Schema.CopyrightToken.Attributes.SongDetails].S));
        }

        public class OldSongModel
        {
            public string Owner { set; get; }

            public string Songwriter { set; get; }

            public string Composer { set; get; }

            public string Lyricist { set; get; }

            public string Producer { set; get; }

            public string Engineer { set; get; }

            public string FeaturedArtist { set; get; }

            public string NonFeaturedMusician { set; get; }

            public string NonFeaturedVocalist { set; get; }
        }
    }
}
