using App.Metrics;
using Confluent.Kafka;
using Kafka.Utilities;
using Microsoft.Extensions.Logging;
using Kafka.Utilities.Extensions;

namespace Kafka.Clients.Consumer;

public class KafkaConsumerBuilderCompositeFactory<TKey, TValue>
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;
    private readonly KafkaConsumer<TKey, TValue> _kafkaConsumer;

    public KafkaConsumerBuilderCompositeFactory(
        ILogger logger,
        IMetrics metrics,
        KafkaConsumer<TKey, TValue> kafkaConsumer)
    {
        _logger = logger;
        _metrics = metrics;
        _kafkaConsumer = kafkaConsumer;
    }

    public IConsumer<TKey, TValue> Build()
    {
        var consumerBuilder = new ConsumerBuilder<TKey, TValue>(_kafkaConsumer.Configuration)
            .SetErrorHandler((client, error) =>
            {
                var tags = new MetricTags(MetricsRegistry.TopicName, client.Subscription.Any() ? string.Join("|", client.Subscription) : "no_subs");
                _metrics?.Measure?.Meter?.Mark(MetricsRegistry.KafkaErrors, tags);
                _logger.LogError("Consumer error: {@Error}", error);
            })
            .SetLogHandler((_, message) =>
                _logger.Log(message.Level.Map(), "Consumer error: {Message}, {Name}", message.Message, message.Name))
            .SetPartitionsAssignedHandler((c, parts) =>
                _logger.LogWarning("Assigned: {0}", string.Join(",", parts)))
            .SetPartitionsLostHandler((c, parts) =>
                _logger.LogWarning("Lost: {0}", string.Join(",", parts)))
            .SetPartitionsRevokedHandler((_, parts) =>
            {
                _logger.LogWarning("Revoked: {0}", string.Join(",", parts));

                //This empty because
                // This will effectively avoid the race condition because rebalance
                // handlers are called as a side effect of a call to Consume
                // on the application thread - the rebalance is effectively
                // blocked whilst you process the consumed message and store or commit offsets
            })
            .SetOffsetsCommittedHandler((_, o) =>
            {
                if (o.Error.IsError)
                {
                    foreach (var offset in o.Offsets.Where(offset => offset.Error.IsError))
                    {
                        _logger.LogError(new Exception(o.Error.ToString()),
                            "Error occurred during OffsetsCommit of offset: {Offset}", offset);
                    }
                }
            });
        
        if (typeof(Ignore) != typeof(TKey) && typeof(TKey) != typeof(string))
        {
            consumerBuilder.SetKeyDeserializer(_kafkaConsumer.KeyDeserializer);
        }

        consumerBuilder.SetValueDeserializer(_kafkaConsumer.ValueDeserializer);

        return consumerBuilder.Build();
    }
}