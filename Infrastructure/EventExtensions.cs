using Infrastructure.Attributes;
using Infrastructure.Bus.Abstractions;
using System;
using System.Linq;

namespace Infrastructure;

public static class EventExtensions
{
    public static string GetBusEventName(this IEvent @event)
    {
        return @event.GetType().GetBusEventName();
    }

    public static string GetBusEventName(this Type type)
    {
        var attribute = type.GetCustomAttributes(typeof(BusEventNameAttribute), true).FirstOrDefault();
        if (attribute is null)
        {
            return type.Name;
        }

        var busEventNameAttribute = (BusEventNameAttribute) attribute;
        var busEventName = busEventNameAttribute.Name;

        return string.IsNullOrEmpty(busEventName) ? type.Name : busEventName;
    }
}