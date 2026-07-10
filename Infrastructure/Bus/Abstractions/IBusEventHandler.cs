using System;
using System.Threading;
using System.Threading.Tasks;

namespace Infrastructure.Bus.Abstractions
{
    public interface IEventHandlerBase
    {
        Type EventType { get; } // todo: refactor?
    }
    
    public interface IEventHandler : IEventHandlerBase
    {
        Task HandleAsync(IEvent @event, CancellationToken cancellationToken);
    }
    
    public interface IEventHandler<in TEvent> where TEvent : IEvent
    {
        Task HandleAsync(TEvent @event, CancellationToken cancellationToken);
    }
}