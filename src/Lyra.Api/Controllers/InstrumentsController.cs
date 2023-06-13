namespace Lyra.Api.Controllers
{
    using System;
    using System.Collections.Concurrent;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;
    using System.Threading.Tasks;
    using Lyra.Api.Models.Instruments;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using MoreLinq;

    public class InstrumentsController : ControllerBase
    {
        private readonly IProductReadRepository productReadRepository;

        private readonly ILogger<InstrumentsController> logger;

        public InstrumentsController(
            IProductReadRepository productReadRepository, ILogger<InstrumentsController> logger)
        {
            this.logger = logger;
            this.productReadRepository = productReadRepository;
        }

        [HttpGet("{tenantId}/instruments/{instrumentId}")]
        public async Task<IActionResult> GetInstrument(string tenantId, string instrumentId)
        {
            var instrument = await productReadRepository.GetInstrumentAsync(tenantId, instrumentId);

            if (instrument == null)
            {

                logger.LogDebug($"instruments not found for tenant: {tenantId}");
                return NotFound();
            }

            return Ok(new InstrumentModel
            {
                Id = instrument,
                Name = instrument.Name,
                NumberOfDecimalPlaces = instrument.NumberOfDecimalPlaces
            });
        }

        [HttpPost("{tenantId}/instruments/batch")]
        public async Task<IActionResult> GetInstruments([FromRoute]string tenantId, [FromBody] InstrumentsRequest request)
        {
            if (request == null || request.Ids == null || request.Ids.Count == 0)
                return BadRequest();

            var instrumentsDict = new ConcurrentDictionary<string, InstrumentModel>();
            var batches = request.Ids.Batch(10);
            foreach (var batch in batches)
            {
                var tasks = batch.Select(id => productReadRepository.GetInstrumentAsync(tenantId, id));
                await Task.WhenAll(tasks);
                foreach(var task in tasks)
                {
                    var instrument = task.Result;
                    instrumentsDict[instrument.Id] = new InstrumentModel
                    {
                        Id = instrument.Id,
                        Name = instrument.Name,
                    };
                }
            }

            return Ok(instrumentsDict.Values);
        }

        [Obsolete("Will not take into the account disabled mechanism. Remove after migrating the app.")]
        [HttpGet("{tenantId}/instruments")]
        public async Task<IActionResult> GetInstruments(string tenantId)
        {
            var instruments = await productReadRepository.GetAllInstrumentsAsync(tenantId);

            var instrumentsList = instruments.ToList();

            logger.LogDebug($"Found {instrumentsList.Count} instruments for tenant {tenantId}");
            return Ok(instrumentsList.Select(x => new InstrumentModel
            {
                Id = x,
                Name = x.Name,
            }));
        }


        [HttpGet("{tenantId}/enabledinstruments/paginated")]
        public async Task<IActionResult> GetEnabledInstrumentsPaginated(
            [FromRoute] string tenantId,
            [FromQuery, Required] int pageSize,
            [FromQuery] string token)
        {
            var instruments = await productReadRepository.GetAllEnabledInstrumentsPaginatedAsync(tenantId, pageSize, token);

            logger.LogDebug($"Found {instruments.Items.Count} instruments for tenant {tenantId}");

            return Ok(instruments);
        }
    }
}
