using System.Diagnostics;

namespace Kafka.Utilities;

public class Telemetry
{
    public const string EventSourceHeaderKey = "event-source";
    public const string BusEventNameHeaderKey = "bus-event-name";
    public const string FailureTime = "failure-time";
    
    public static void AddActivityTag(string attribute, string value)
    {
        Activity.Current?.AddTag(attribute, value);
    }
}