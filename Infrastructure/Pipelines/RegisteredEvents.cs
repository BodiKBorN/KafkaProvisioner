using Infrastructure.Bus.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Infrastructure.Pipelines;

internal interface IRegisteredEvents
{
    Type GetEventDescription(string eventName);
}

internal interface IRegisteredEventHandlers
{
    Type GetEventHandlerDescription(Type eventType);
}

internal class RegisteredEvents : IRegisteredEvents
{
    private readonly IDictionary<string, Type> _registeredEventsDictionary;

    public RegisteredEvents(IEnumerable<IEvent> events)
    {
        _registeredEventsDictionary = events.DistinctBy(x => x.GetBusEventName())
            .ToDictionary(x => x.GetBusEventName(), x => x.GetType());
    }

    public Type GetEventDescription(string eventName)
    {
        if (string.IsNullOrEmpty(eventName))
        {
            return null;
        }

        return _registeredEventsDictionary.TryGetValue(eventName, out var value) ? value : null;
    }
}

public class RegisteredEventHandlers : IRegisteredEventHandlers
{
    private readonly IDictionary<string, Type> _registeredEventHandlersDictionary;

    public RegisteredEventHandlers(IEnumerable<IEventHandlerBase> busEventHandlers)
    {
        _registeredEventHandlersDictionary = busEventHandlers
            .DistinctBy(x => x.EventType.GetBusEventName())
            .ToDictionary(eventHandler => eventHandler.EventType.GetBusEventName(), eventHandler => eventHandler.GetType());
    }

    public Type GetEventHandlerDescription(Type eventType)
    {
        if (eventType is null)
        {
            return null;
        }

        var eventName = eventType.GetBusEventName();
        return _registeredEventHandlersDictionary.TryGetValue(eventName, out var value) ? value : null;
    }
}