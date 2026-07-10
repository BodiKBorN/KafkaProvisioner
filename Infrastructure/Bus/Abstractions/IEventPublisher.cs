using System.Collections.Generic;
using System.Threading.Tasks;
using Tech.Social.Domain;

namespace Tech.Social.Infrastructure.Bus.Abstractions;

public interface IEventPublisher
{
    Task PublishNonKeyedEventAsync(IEvent busEvent);

    Task PublishErrorAsync(IEvent busEvent, string message, int retryCount = 0);

    Task PublishEventAsync(IEvent busEvent);

    Task PublishEventsAsync(IEnumerable<IEvent> busEvents);
}