using App.Metrics;
using App.Metrics.Meter;
using App.Metrics.Timer;

namespace Kafka.Utilities;

internal static class MetricsRegistry
{
    public const string EventName = "event_name";
    public const string TopicName = "topic_name";
    public const string ConsumerGroup = "consumer_group";

    public static readonly TimerOptions Consume = new()
    {
        Name = "Kafka.Consume",
        MeasurementUnit = Unit.Calls,
        DurationUnit = TimeUnit.Seconds,
        RateUnit = TimeUnit.Seconds
    };
    
    public static readonly TimerOptions Publish = new()
    {
        Name = "Kafka.Publish",
        MeasurementUnit = Unit.Calls,
        DurationUnit = TimeUnit.Seconds,
        RateUnit = TimeUnit.Seconds
    };

    public static readonly TimerOptions ConsumerWait = new()
    {
        Name = "Kafka.Consumer.Wait",
        MeasurementUnit = Unit.Calls,
        DurationUnit = TimeUnit.Seconds,
        RateUnit = TimeUnit.Seconds
    };

    public static readonly MeterOptions ConsumeExceptionCounter = new()
    {
        Name = "Kafka Consume Exception Counter",
        MeasurementUnit = Unit.Errors
    };

    public static readonly MeterOptions KafkaErrors = new()
    {
        Name = "Kafka Errors Counter",
        MeasurementUnit = Unit.Errors
    };

    public static MetricTags CreateEventNameTag(this string eventName, string topicName) => new(new[] {EventName, TopicName}, new[] {eventName, topicName});

    public static MetricTags GetMessageMetricTags(string topic, string eventName, string consumerGroup)
    {
        return new MetricTags(new[]
            {
                TopicName,
                EventName,
                ConsumerGroup
            },
            new[]
            {
                topic,
                eventName,
                consumerGroup
            });
    }
}