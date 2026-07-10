using Confluent.Kafka;

namespace Kafka.Clients.Producer;

public interface IKafkaProducer<TKey, TValue> : IDisposable
{
    Task<TopicPartitionOffset> ProduceAsync(TKey messageKey, TValue message, Dictionary<string, string> header = null);

    Task<TopicPartitionOffset> ProduceAsync(TValue message, Dictionary<string, string> header = null);

    Task<TopicPartitionOffset> ProduceAsync(Message<TKey, TValue> message);

    Task<TopicPartitionOffset> ProduceErrorAsync(TValue message);
}