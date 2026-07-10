using System;
using System.Text;
using System.Threading;
using Tech.Domain;
using Tech.Kafka.Utilities;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using Tech.Social.Domain;

namespace Tech.Social.Infrastructure.Pipelines;

internal class EventHandlerResult
{
    public Func<Task> EventHandler { get; private init; }
    public string HandlerType { get; private init; }
    public bool IsSuccess { get; private init; }

    public static EventHandlerResult Ok(Func<Task> eventHandler, string handlerType) => new()
    {
        EventHandler = eventHandler,
        HandlerType = handlerType,
        IsSuccess = true
    };

    public static EventHandlerResult Fail() => new()
    {
        IsSuccess = false
    };
}

internal interface IMessageDeserialize
{
    public EventHandlerResult GetEventHandlerFromMessage(
        Message<string, JObject> message,
        IServiceScope serviceScope,
        CancellationToken cancellationToken);
}

internal class MessageDeserialize : IMessageDeserialize
{
    private readonly ILogger<MessageDeserialize> _logger;
    private readonly IRegisteredEvents _registeredEvents;

    public MessageDeserialize(
        ILogger<MessageDeserialize> logger,
        IRegisteredEvents registeredEvents)
    {
        ArgumentNullException.ThrowIfNull(registeredEvents);
        ArgumentNullException.ThrowIfNull(logger);
        
        _registeredEvents =  registeredEvents;
        _logger = logger;
    }
    
    public EventHandlerResult GetEventHandlerFromMessage(
        Message<string, JObject> message,
        IServiceScope scope,
        CancellationToken cancellationToken)
    {
        var eventName = string.Empty;
        try
        {
            eventName = GetEventNameFromHeader(message);
            var eventType = _registeredEvents.GetEventDescription(eventName);
            if (eventType == null)
                return EventHandlerResult.Fail();

            var @event = GetEventDeserialize(message, eventType);
            var (eventHandler, handlerName) = GetEventHandler(eventType, scope);

            return EventHandlerResult.Ok(() => eventHandler.HandleAsync(@event, cancellationToken), handlerName);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MessageDeserialize fail. EventName: {@eventName}", eventName);
        }
        
        return EventHandlerResult.Fail();
    }

    private string GetEventNameFromHeader(Message<string, JObject> message)
    {
        if (!message.Headers.TryGetLastBytes(Telemetry.BusEventNameHeaderKey, out var eventBusNameHeader))
        {
            throw new ArgumentException($"No {Telemetry.BusEventNameHeaderKey} header found for event");
        }
                
        return Encoding.UTF8.GetString(eventBusNameHeader);
    }

    private (IEventHandler EventHandler, string HandlerName) GetEventHandler(Type eventType,  IServiceScope scope)
    {
        var registeredEventHandlers = scope.ServiceProvider.GetService<IRegisteredEventHandlers>();
        var handlerType = registeredEventHandlers.GetEventHandlerDescription(eventType);

        if (handlerType == null || scope.ServiceProvider.GetService(handlerType) is not IEventHandler eventHandler)
        {
            throw new ArgumentException($"Can't resolve event handler type {eventType.Name}");
        }

        Telemetry.AddActivityTag("event-handler", handlerType.ToString());

        return (eventHandler, handlerType.Name);
    }
    
    private IEvent GetEventDeserialize(Message<string, JObject> message, Type eventType)
    {
        if (!TryDeserialize(message.Value, eventType, out var deserialized, out var ex))
        {
            const int maxJsonEventLength = 255;
            var data = message.Value.ToString();
            var jsonEvent = data.Length > maxJsonEventLength ? data[..maxJsonEventLength] : data;
            
            throw new ArgumentException($"{ex.Message} BusEventName: {eventType?.Name}; Event: {jsonEvent}");
        }
        
        if (deserialized is not IEvent @event)
        {
            throw new ArgumentException($"Can't deserialize EventName={eventType?.Name}");
        }
        
        return @event;
    }
    
    private bool TryDeserialize(JObject data, Type eventType, out object result, out Exception exception)
    {
        try
        {
            result = data.ToObject(eventType);
            exception = null;
                
            return true;
        }
        catch (Exception ex)
        {
            result = null;
            exception = ex;
                
            return false;
        }
    }
}