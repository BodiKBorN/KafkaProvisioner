using Confluent.Kafka;
using System.Text;
using Tech.Kafka.Utilities;

namespace Infrastructure.Utilities;

public static class MessageExtensions
{
    public static string GetEventBusName(this MessageMetadata message) =>
        message.Headers.TryGetLastBytes(Telemetry.BusEventNameHeaderKey, out var eventBusNameHeader) ?
        Encoding.UTF8.GetString(eventBusNameHeader) :
        string.Empty;
}