using System.Diagnostics;
using App.Metrics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Kafka.Utilities;

public interface IKafkaMessagePropagator<TKey, TValue>
{
    Activity StartActivity(ConsumeResult<TKey, TValue> result);
    MetricTags GetMessageMetrics(ConsumeResult<TKey, TValue> result);
    
    ILogger Logger { get; }
}