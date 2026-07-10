using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text;
using Tech.Kafka.Utilities;

namespace Infrastructure.Pipelines.Assign;

public abstract class BaseAssignEventHandler<TKey, TValue>
{
    private readonly ILogger _logger;

    protected BaseAssignEventHandler(ILogger logger)
    {
        _logger = logger;
    }

    public string GetNameEvent(ConsumeResult<TKey, TValue> message)
    {
        if (!message.Message.Headers.TryGetLastBytes(Telemetry.BusEventNameHeaderKey, out var eventBusNameHeader))
        {
            _logger.LogError($"No {Telemetry.BusEventNameHeaderKey} header found for event");
        }

        return Encoding.UTF8.GetString(eventBusNameHeader);
    }
}