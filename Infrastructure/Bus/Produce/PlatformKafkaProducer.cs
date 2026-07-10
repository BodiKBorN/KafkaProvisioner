using Infrastructure.Bus.Abstractions;
using Infrastructure.Bus.Produce.ProducerInvokers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Infrastructure.Bus.Produce;

internal interface IPlatformKafkaProducer
{
    Task ProduceAsync(IEvent message, Dictionary<string, string> header = null);

    Task ProduceWithoutKeyAsync(IEvent message, Dictionary<string, string> header = null);

    Task ProduceErrorAsync(IEvent errorMessage, string message, int retryCount = 0, Dictionary<string, string> header = null);
}

internal class PlatformKafkaProducer(
    NonKeyedProducerInvokerRegistry nonKeyedProducerInvokerRegistry,
    ProducerInvokerRegistry producerInvokerRegistry,
    ErrorProducerInvokerRegistry errorProducerInvokerRegistry,
    IEnumerable<IEventKeyMapper> eventKeyMappers,
    ILogger<PlatformKafkaProducer> logger)
    : IPlatformKafkaProducer
{
    public async Task ProduceAsync(IEvent message, Dictionary<string, string> header = null)
    {
        try
        {
            var producerInvoker = producerInvokerRegistry.GetInvoker(message);

            var messageKey = eventKeyMappers
                                 .Select(m => m.GetKey(message)).LastOrDefault(k => !string.IsNullOrEmpty(k)) ??
                             Guid.NewGuid().ToString();

            await producerInvoker.ProduceAsync(messageKey, message, header);
        }
        catch(Exception ex)
        {
            logger.LogError(ex, "Failed to publish event: {EventName}", message.GetType().Name);
            throw;
        }
    }

    public async Task ProduceWithoutKeyAsync(IEvent message, Dictionary<string, string> header = null)
    {
        try
        {
            var producerInvoker = nonKeyedProducerInvokerRegistry.GetInvoker(message);

            await producerInvoker.ProduceAsync(message, header);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to publish event: {EventName}", message.GetType().Name);
            throw;
        }
    }

    public async Task ProduceErrorAsync(
        IEvent sourceEvent,
        string errorMessage,
        int retryCount = 0,
        Dictionary<string, string> header = null)
    {
        try
        {
            var producerInvoker = errorProducerInvokerRegistry.GetInvoker(sourceEvent);

            var messageKey = eventKeyMappers
                                 .Select(m => m.GetKey(sourceEvent)).LastOrDefault(k => !string.IsNullOrEmpty(k));

            await producerInvoker.ProduceAsync(messageKey, sourceEvent, errorMessage, retryCount, header);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to publish event: {EventName}", sourceEvent.GetType().Name);
            throw;
        }
    }
}