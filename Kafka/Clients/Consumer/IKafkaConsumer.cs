using Confluent.Kafka;
using Kafka.Utilities;

namespace Kafka.Clients.Consumer;

public interface IKafkaConsumer<TKey, TValue> : IDisposable
{
    IKafkaMessagePropagator<TKey, TValue> MessagePropagator { get; }
    
    void BuildConsumer(Action<ConsumerConfig>? configureAction = null);

    void StoreOffsets(params TopicPartitionOffset[] topicPartitionOffsets);

    void StoreOffsets(params ConsumeResult<TKey, TValue>[] results);

    IObservable<ConsumeResult<TKey, TValue>> Subscribe(SeekOrigin seekOrigin = SeekOrigin.Current);

    IObservable<ConsumeResult<TKey, TValue>> Assign(Offset offset);
}