using System;
using System.Threading;
using System.Threading.Tasks;
using Tech.Common.Extensions;

namespace Tech.Social.Domain
{
    public abstract class EventHandlerBase<TEvent> : IEventHandler where TEvent : class, IEvent
    {
        public Type EventType => typeof(TEvent);

        protected abstract Task HandleAsync(TEvent busEvent, CancellationToken cancellationToken);

        public async Task HandleAsync(IEvent @event, CancellationToken cancellationToken)
        {
            @event = @event.NotNull(nameof(@event));
            if (@event is not TEvent concreteEvent)
            {
                throw new Exception(
                    $"Handler receive wrong event type. Expected {typeof(TEvent)}. Actual {@event.GetType()}");
            }

            await HandleAsync(concreteEvent, cancellationToken);
        }
        
    }
}