using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Tech.Social.Domain;
using Tech.Social.Infrastructure.Bus.Abstractions;

namespace Tech.Social.Infrastructure.Bus.Produce;

internal sealed class EventPublisher(
    IPlatformKafkaProducer platformKafkaProducer,
    ILogger<EventPublisher> logger)
    : IEventPublisher
{
    public Task PublishNonKeyedEventAsync(IEvent busEvent)
    {
        ArgumentNullException.ThrowIfNull(busEvent);

        return platformKafkaProducer.ProduceWithoutKeyAsync(busEvent);
    }

    public async Task PublishErrorAsync(IEvent busEvent, string message, int retryCount = 0)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(busEvent);

            await platformKafkaProducer.ProduceErrorAsync(busEvent, message, retryCount);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to publish error to DLQ for event {EventType}", busEvent?.GetType().Name);
        }
    }

    public Task PublishEventAsync(IEvent busEvent)
    {
        ArgumentNullException.ThrowIfNull(busEvent);

        return platformKafkaProducer.ProduceAsync(busEvent);
    }

    public async Task PublishEventsAsync(IEnumerable<IEvent> busEvents)
    {
        ArgumentNullException.ThrowIfNull(busEvents);
        
        foreach (var page in busEvents.Chunk(100))
        {
            await Task.WhenAll(page.Select(e => platformKafkaProducer.ProduceAsync(e)));
        }
    }
}