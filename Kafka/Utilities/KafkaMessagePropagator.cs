using System.Diagnostics;
using System.Text;
using App.Metrics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Kafka.Utilities;

public class KafkaMessagePropagator<TKey, TValue> : IKafkaMessagePropagator<TKey, TValue>
{
    private static readonly ActivitySource ActivitySource = new(nameof(KafkaMessagePropagator<TKey, TValue>));
    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;
    
    private readonly string _groupId;

    public KafkaMessagePropagator(string groupId, ILogger logger)
    {
        _groupId = groupId;
        Logger = logger;
    }

    public ILogger Logger { get; }

    public Activity StartActivity(ConsumeResult<TKey, TValue> result)
    {
        var parentContext = Propagator.Extract(default, result.Message.Headers, ExtractTraceContextFromHeaders);
        Baggage.Current = parentContext.Baggage;
        var activityName = $"{result.Topic} receive";
        var activity = ActivitySource.StartActivity(activityName, ActivityKind.Consumer, parentContext.ActivityContext);
        return activity.EnrichWithMessagingTags(result, _groupId);
    }

    public MetricTags GetMessageMetrics(ConsumeResult<TKey, TValue> result)
    {
        var eventName = ExtractTraceContextFromHeaders(result.Message.Headers, Telemetry.BusEventNameHeaderKey).FirstOrDefault();
        if (string.IsNullOrEmpty(eventName))
        {
            return MetricTags.Empty;
        }
        return MetricsRegistry.GetMessageMetricTags(result.Topic, eventName, _groupId);
    }

    private IEnumerable<string> ExtractTraceContextFromHeaders(Headers headers, string key)
    {
        try
        {
            if (headers.TryGetLastBytes(key, out var value))
            {
                return new[] {Encoding.UTF8.GetString(value)};
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to extract trace context from headers");
        }

        return Enumerable.Empty<string>();
    }
}