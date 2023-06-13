namespace Lyra.ManagementApi.Controllers
{
    using Lyra.Api.Models.Instruments;
    using Lyra.Instruments;
    using Lyra.ManagementApi.Models;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using MoreLinq;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Concurrent;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;
    using System.Threading.Tasks;

    public class InstrumentsController : ControllerBase
    {
        #region declaration
        private readonly IProductReadRepository _productReadRepository;
        private readonly ILogger<InstrumentsController> _logger;
        private readonly IProductWriteRepository _productWriteRepository;
        #endregion

        #region constructor
        public InstrumentsController(
            IProductReadRepository productReadRepository,
            IProductWriteRepository productWriteRepository,
            ILogger<InstrumentsController> logger)
        {
            _productWriteRepository = productWriteRepository ?? throw new ArgumentNullException(nameof(productWriteRepository));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _productReadRepository = productReadRepository ?? throw new ArgumentNullException(nameof(productReadRepository));
        }

        #endregion

        #region public Method API
        [HttpGet("{tenantId}/instruments/{instrumentId}")]
        public async Task<IActionResult> GetInstrument(string tenantId, string instrumentId)
        {
            try
            {
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, instrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {instrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                return Ok(instrument);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while retreiving the instrument with id {instrumentId} for tenant {tenantId}");
                return BadRequest(new
                {
                    ex.Message
                });
            }
        }

        [HttpPost("{tenantId}/instruments/batch")]
        public async Task<IActionResult> GetInstruments([FromRoute] string tenantId, [FromBody] InstrumentsRequest request)
        {
            if (request == null || request.Ids == null || request.Ids.Count == 0)
                return BadRequest();

            var instrumentsDict = new ConcurrentDictionary<string, InstrumentModel>();
            var batches = request.Ids.Batch(10);
            foreach (var batch in batches)
            {
                var tasks = batch.Select(id => _productReadRepository.GetInstrumentAsync(tenantId, id));
                await Task.WhenAll(tasks);
                foreach (var task in tasks)
                {
                    var instrument = task.Result;
                    try
                    {
                        if (instrument != null)
                        {
                            instrumentsDict[instrument.Id] = new InstrumentModel
                            {
                                Id = instrument.Id,
                                Name = instrument.Name,
                                InstrumentStatus = instrument.InstrumentStatus,
                                NumberOfDecimalPlaces = instrument.NumberOfDecimalPlaces
                            };
                        }
                    }
                    catch (Exception ex)
                    {
                        var attributes = JsonConvert.SerializeObject(instrument);
                        _logger.LogError("Exception ocurred while feteching the instrument and the message is  {Exception} and the serialized message is {Attribute} for tenant {TenantId}", ex, attributes, tenantId);
                    }
                }

            }
            return Ok(instrumentsDict.Values);
        }

        [HttpGet("{tenantId}/instruments")]
        public async Task<IActionResult> GetInstruments(string tenantId)
        {
            try
            {
                var instruments = await _productReadRepository.GetAllInstrumentsAsync(tenantId);

                if (instruments == null)
                {
                    var errorContent = $"instruments not found for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }
                var instrumentsList = instruments.ToList();

                _logger.LogDebug($"Found {instrumentsList.Count} instruments for tenant {tenantId}");
                return Ok(instruments);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while retreiving the instruments for tenant {tenantId}");
                return BadRequest(new
                {
                    ex.Message
                });
            }
        }


        [HttpGet("{tenantId}/instruments/paginated")]
        public async Task<IActionResult> GetInstruments(
            [FromRoute] string tenantId,
            [FromQuery, Required] int pageSize,
            [FromQuery] string token)
        {
            try
            {
                var instruments = await _productReadRepository.GetAllInstrumentsPaginatedAsync(tenantId, pageSize, token);

                if (instruments == null)
                {
                    var errorContent = $"instruments not found for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                _logger.LogDebug($"Found {instruments.Items.Count} instruments for tenant {tenantId}");

                return Ok(instruments);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while retreiving the instruments for tenant {tenantId}");
                return BadRequest(new
                {
                    ex.Message
                });
            }
        }

        [HttpPost("{tenantId}/instrument")]
        public async Task<IActionResult> Create([FromRoute] string tenantId, [FromBody] CreateInstrumentRequest request)
        {
            try
            {
                if (request == null)
                {
                    var errorContent = $"instrument creation request is empty for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return BadRequest(errorContent);
                }

                await _productWriteRepository.AddInstrumentAsync(tenantId, new Instrument(request.InstrumentId, request.Name, request.InstrumentStatus, request.NumberOfDecimalPlaces));

                return Created($"{tenantId}/instrument/{request.InstrumentId}", request);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while creating the instrument with Id {request.InstrumentId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpDelete("{tenantId}/instrument/{id}")]
        public async Task<IActionResult> Delete([FromRoute] string tenantId, [FromRoute] string id)
        {
            await _productWriteRepository.RemoveInstrumentAsync(tenantId, id);

            return new OkResult();
        }

        [HttpPut("{tenantId}/instrument/{instrumentId}")]
        public async Task<IActionResult> UpdateInstrument([FromRoute] string tenantId,
           [FromRoute] string instrumentId, [FromBody] UpdateInstrumentRequest request)
        {

            _logger.LogDebug($"Received request to Update instrument with Id {instrumentId} for tenant {tenantId}");

            try
            {
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, instrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {instrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                await _productWriteRepository.UpdateInstrumentAsync(tenantId, instrumentId, new Instrument(instrument.Id, request.Name, instrument.InstrumentStatus, request.NumberOfDecimalPlaces));
                _logger.LogDebug($"Updated instrument with Id: {instrument.Id} for tenant {tenantId}");
                return Ok();

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while Updating the instrument with Id {instrumentId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/instrument/{instrumentId}/disable")]
        public async Task<IActionResult> DisableInstrument([FromRoute] string tenantId,
           [FromRoute] string instrumentId)
        {

            _logger.LogDebug($"Received request to disable instrument Status with Id {instrumentId} for tenant {tenantId}");

            try
            {
                var status = (int)InstrumentStatus.Disabled;
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, instrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {instrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }
                await _productWriteRepository.UpdateInstrumentStatusAsync(tenantId, instrument, status);
                _logger.LogDebug($"Updated instrument Status {instrument.Id} for tenant {tenantId}");
                return Ok();

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while disabling the instrument Status with Id {instrumentId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        [HttpPut("{tenantId}/instrument/{instrumentId}/enable")]
        public async Task<IActionResult> EnableInstrument([FromRoute] string tenantId,
          [FromRoute] string instrumentId)
        {

            _logger.LogDebug($"Received request to enable instrument Status with Id {instrumentId} for tenant {tenantId}");

            try
            {
                var status = (int)InstrumentStatus.Enabled;
                var instrument = await _productReadRepository.GetInstrumentAsync(tenantId, instrumentId);

                if (instrument == null)
                {
                    var errorContent = $"instruments not found with instrument id {instrumentId} for tenant {tenantId}";
                    _logger.LogDebug(errorContent);
                    return NotFound(errorContent);
                }

                await _productWriteRepository.UpdateInstrumentStatusAsync(tenantId, instrument, status);
                _logger.LogDebug($"Updated instrument Status {instrument.Id} for tenant {tenantId}");
                return Ok();

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Issue while disabling the instrument Status with Id {instrumentId} for tenant {tenantId}");
                return BadRequest(new { ex.Message });
            }
        }

        #endregion
    }
}
