using System;
using System.Collections.Generic;

namespace Infrastructure.Bus.Abstractions;

public interface IEventKeyMapper
{
    string? GetKey(IEvent busEvent);
}

public abstract class EventKeyMapperBase(
    Dictionary<Type, Func<IEvent, string>> mappings)
    : IEventKeyMapper
{
    public string GetKey(IEvent busEvent)
    {
        var type = busEvent.GetType();
        return mappings.TryGetValue(type, out var value)
            ? value(busEvent)
            : default;
    }
}