using Confluent.Kafka;
using Kafka.Clients.Consumer;
using Kafka.Clients.Producer;

namespace Kafka.Clients;

public interface IKafkaClientFactory : IDisposable
{
    IKafkaConsumer<TKey, TValue> GetConsumer<TKey, TValue>(
        string topic,
        string consumeGroup,
        bool enableAutoCommit = true,
        bool enableAutoOffsetStore = false,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Latest);

    IKafkaProducer<TKey, TValue> GetProducer<TKey, TValue>(string topic,
        string serviceGroupId,
        ISerializer<TKey>? keyDeserializer = null,
        ISerializer<TValue>? valueDeserializer = null,
        KafkaProducerConfig? kafkaProducerConfig = null);
}