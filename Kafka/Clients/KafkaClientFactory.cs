using System.Collections.Concurrent;
using App.Metrics;
using Confluent.Kafka;
using Kafka.Clients.Consumer;
using Kafka.Clients.Producer;
using Microsoft.Extensions.Logging;

namespace Kafka.Clients;

public class KafkaClientFactory : IKafkaClientFactory
{
    private readonly ConcurrentBag<IDisposable> _clients = new ();
    private readonly string _broker;
    private readonly IMetrics _metrics;
    private readonly ILogger _logger;

    public KafkaClientFactory(string broker, IMetrics metrics, ILogger logger)
    {
        _broker = broker ?? throw new ArgumentNullException(nameof(broker));
        _metrics = metrics;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }   
    
    public IKafkaConsumer<TKey, TValue> GetConsumer<TKey, TValue>(
        string topic,
        string consumeGroup,
        bool enableAutoCommit = true,
        bool enableAutoOffsetStore = false,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Latest)
    {
        var consumer = new KafkaConsumer<TKey, TValue>(
            _broker,
            topic,
            consumeGroup,
            _logger,
            _metrics);
            
        consumer.BuildConsumer(config =>
        {
            config.AutoOffsetReset = autoOffsetReset;
            config.EnableAutoOffsetStore = enableAutoOffsetStore;
            config.EnableAutoCommit = enableAutoCommit;
            config.QueuedMinMessages = 1000;
            config.QueuedMaxMessagesKbytes = 10000;
        });
        _clients.Add(consumer);

        return consumer;
    }

    public IKafkaProducer<TKey, TValue> GetProducer<TKey, TValue>(string topic,
        string serviceGroupId,
        ISerializer<TKey>? keyDeserializer = null,
        ISerializer<TValue>? valueDeserializer = null,
        KafkaProducerConfig? kafkaProducerConfig = null)
    {
        var producer = new KafkaProducer<TKey, TValue>(
            _broker,
            topic,
            serviceGroupId,
            _logger,
            _metrics,
            kafkaProducerConfig);

        producer.BuildProducer(keyDeserializer: keyDeserializer, valueDeserializer:valueDeserializer);

        _clients.Add(producer);
        return producer;
    }

    public void Dispose()
    {
        foreach (var client in _clients)
        {
            client?.Dispose();
        }
    }
}