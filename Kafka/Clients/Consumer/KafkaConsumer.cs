using App.Metrics;
using Confluent.Kafka;
using Kafka.Clients.Admin;
using Kafka.Serializers.Json;
using Kafka.Utilities;
using Microsoft.Extensions.Logging;
using System.Reactive.Subjects;

namespace Kafka.Clients.Consumer;

public class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;
    private readonly string _topic;
    private readonly KafkaAdminClient _kafkaAdminClient;
    private readonly TaskCompletionSource<bool> _consumerCompleted = new ();
    private readonly CancellationTokenSource _cancellationTokenSource = new ();
    private readonly Subject<ConsumeResult<TKey, TValue>> _consumerSubject = new ();

    public KafkaConsumer(
        string bootstrapServers,
        string topic,
        string groupId,
        ILogger logger,
        IMetrics metrics,
        IDeserializer<TKey>? keyDeserializer = null,
        IDeserializer<TValue>? valueDeserializer = null)
    {
        _logger = logger;
        _metrics = metrics;
        _topic = topic;
        Configuration = new ConsumerConfig()
        {
            AllowAutoCreateTopics = false,
            BootstrapServers = bootstrapServers,
            ClientId = Environment.MachineName,
            EnableAutoOffsetStore = false,
            GroupId = groupId
        };
        KeyDeserializer = keyDeserializer ?? new JsonDataSerializer<TKey>();
        ValueDeserializer = valueDeserializer ?? new JsonDataSerializer<TValue>();
        _kafkaAdminClient = new KafkaAdminClient(bootstrapServers);
        MessagePropagator = new KafkaMessagePropagator<TKey, TValue>(groupId, logger);
    }
    
    public IKafkaMessagePropagator<TKey, TValue> MessagePropagator { get; private set;}
    
    public IConsumer<TKey, TValue> Consumer { get; set; }
    
    public SeekOrigin SeekOrigin { get; private set; }        
    
    public ConsumerConfig Configuration { get; }
    
    public IDeserializer<TKey> KeyDeserializer { get; }
    
    public IDeserializer<TValue> ValueDeserializer { get; }

    public void BuildConsumer(Action<ConsumerConfig>? configureAction = null)
    {
        configureAction?.Invoke(Configuration);
        Consumer = new KafkaConsumerBuilderCompositeFactory<TKey, TValue>
            (_logger, _metrics, this)
            .Build();
    }
    
    public IObservable<ConsumeResult<TKey, TValue>> Subscribe(SeekOrigin seekOrigin = SeekOrigin.Current)
    {
        SeekOrigin = seekOrigin;
        if (Consumer.Subscription.Contains(_topic))
        {
            return _consumerSubject;
        }

        Start();

        Consumer.Subscribe(new[] { _topic });

        return _consumerSubject;
    }
    
    public IObservable<ConsumeResult<TKey, TValue>> Assign(Offset offset)
    {
        if (Consumer.Subscription.Contains(_topic))
        {
            return _consumerSubject;
        }

        Start();

        var partitions = _kafkaAdminClient.GetAllPartitions(_topic);
        var offsets = partitions?.Select(e => new TopicPartitionOffset(e, offset));
        Consumer.Assign(offsets);

        return _consumerSubject;
    }
    
    //add method commit
    
    public void StoreOffsets(params TopicPartitionOffset[] topicPartitionOffsets)
    {
        if (topicPartitionOffsets == null)
        {
            throw new ArgumentNullException(nameof(topicPartitionOffsets));
        }

        TryStoreOffsets(() =>
        {
            foreach (var offset in topicPartitionOffsets
                         .GroupBy(o => o.TopicPartition)
                         .Select(v => v.MaxBy(o => o.Offset.Value)))
            {
                Consumer.StoreOffset(new TopicPartitionOffset(offset.Topic, offset.Partition, offset.Offset + 1));
            }
        });
    }
    
    public void StoreOffsets(params ConsumeResult<TKey, TValue>[] results)
    {
        if (results == null)
        {
            throw new ArgumentNullException(nameof(results));
        }

        TryStoreOffsets(() =>
        {
            foreach (var result in results
                         .GroupBy(o => o.TopicPartition)
                         .Select(v => v.MaxBy(o => o.Offset.Value)))
            {
                Consumer.StoreOffset(result);
            }
        });
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        _consumerCompleted.Task.Wait();

        try
        {
            Consumer?.Close();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during consumer close");
        }
        Consumer?.Dispose();
    }

    private void Start()
    {
        Task.Factory.StartNew(_ => StartInternal(), _cancellationTokenSource.Token, TaskCreationOptions.LongRunning);
    }
    
    private void StartInternal()
    {
        while (!_cancellationTokenSource.IsCancellationRequested)
        {
            try
            {
                var consumeWaitTags = new MetricTags(
                    new[] {MetricsRegistry.TopicName, MetricsRegistry.ConsumerGroup},
                    new[] { _topic, Configuration.GroupId}
                );

                ConsumeResult<TKey, TValue> result;
                using (_metrics?.Measure?.Timer?.Time(MetricsRegistry.ConsumerWait, consumeWaitTags))
                {
                    result = Consumer.Consume(_cancellationTokenSource.Token);
                }

                if (result == null)
                {
                    continue;
                }

                _consumerSubject.OnNext(result);
            }
            catch (ConsumeException e)
            {
                if (e.Error.IsFatal)
                {
                    // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                    throw;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Consume error: {@Error}", e);
            }
        }
        
        _consumerCompleted.SetResult(true);
    }

    private void TryStoreOffsets(Action action)
    {
        try
        {
            action();
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_UnknownPartition 
                                        || ex.Error.Code == ErrorCode.Local_State) { }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error during StoreOffsets {Consumer} in the topic {Topic}",
                Configuration.GroupId, _topic);
        }
    }
}