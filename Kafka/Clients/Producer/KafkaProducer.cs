using System.Globalization;
using System.Text;
using App.Metrics;
using Kafka.Utilities.Extensions;
using Confluent.Kafka;
using Kafka.Serializers.Json;
using Kafka.Utilities;
using Microsoft.Extensions.Logging;
using Polly;

namespace Kafka.Clients.Producer;

public class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;

    private readonly string _topic;
    private readonly string _serviceGroupId;
    
    private readonly ProducerConfig _configuration;

    private readonly AsyncPolicy _retryPolicy;

    public KafkaProducer(
        string brokerHost,
        string topic,
        string serviceGroupId,
        ILogger logger,
        IMetrics metrics,
        KafkaProducerConfig? kafkaProducerConfig = null)
    {
        if (brokerHost == null)
        {
            throw new ArgumentNullException(nameof(brokerHost));
        }

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentNullException(nameof(topic), "Topic should not be empty");
        }

        kafkaProducerConfig ??= new KafkaProducerConfig();

        _logger = logger;
        _topic = topic;
        _metrics = metrics;
        _serviceGroupId = serviceGroupId;
        
        _retryPolicy = Policy
            .Handle<KafkaException>(ex => ex.Error.Code == ErrorCode.Local_QueueFull)
            .WaitAndRetryForeverAsync(
                retryAttempt => TimeSpan.FromMilliseconds(Math.Pow(2, retryAttempt)),
                (exception, span, attempt) => _logger.LogWarning(exception, "Error retrying produce in {span}. Attempt: {attempt}", span, attempt));

        _configuration = new ProducerConfig
        {
            BootstrapServers = brokerHost,
            MessageSendMaxRetries = 64,
            Acks = Acks.All,                        // Wait for all in-sync replicas
            QueueBufferingMaxKbytes = 4096,         // local buffer (helps with spikes)
            RequestTimeoutMs = 15000,               // Timeout per produce attempt (ms)
            ClientId = Environment.MachineName,
            CompressionType = CompressionType.Lz4,  // Low-latency compression
            EnableIdempotence = true,               // Prevent duplicates and preserve ordering
            RetryBackoffMs = 100,                   // Delay between retries (ms)
        };

        if (kafkaProducerConfig.LingerMs.HasValue == true)
        {
            _configuration.LingerMs = kafkaProducerConfig.LingerMs.Value;
        }
    }
    
    public IProducer<TKey, TValue> Producer { get; set; }
    
    public Task<TopicPartitionOffset> ProduceErrorAsync(TValue message)
    {
        return ProduceAsync(message, new Dictionary<string, string>
        {
            [Telemetry.FailureTime] = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture)
        });
    }
    
    public async Task<TopicPartitionOffset> ProduceAsync(TValue message, Dictionary<string, string> header = null)
    {
        var kafkaMessage = new Message<TKey, TValue>
        {
            Value = message,
            Headers = CreateKafkaHeaders(header)
        };

        return await ProduceAsync(kafkaMessage);
    }
    
    public async Task<TopicPartitionOffset> ProduceAsync(TKey messageKey, TValue message, Dictionary<string, string> header = null)
    {
        var kafkaMessage = new Message<TKey, TValue>
        {
            Key = messageKey,
            Value = message,
            Headers = CreateKafkaHeaders(header)
        };

        return await ProduceAsync(kafkaMessage);
    }

    public void BuildProducer(Action<ProducerConfig>? configureAction = null,
        ISerializer<TKey>? keyDeserializer = null,
        ISerializer<TValue>? valueDeserializer = null)
    {
        configureAction?.Invoke(_configuration);
        Producer = new ProducerBuilder<TKey, TValue>(_configuration)
            .SetLogHandler((_, message) => _logger.Log(message.Level.Map(), "Producer error: {Message}, {Name}", message.Message, message.Name))
            .SetKeySerializer(keyDeserializer ?? new JsonDataSerializer<TKey>())
            .SetValueSerializer(valueDeserializer ?? new JsonDataSerializer<TValue>())
            .Build();
    }

    public async Task<TopicPartitionOffset> ProduceAsync(Message<TKey, TValue> message)
    {
        var produceTags = new MetricTags(
            [MetricsRegistry.TopicName],
            [_topic]
        );

        using (_metrics?.Measure?.Timer?.Time(MetricsRegistry.Publish, produceTags))
        {
            var result = await _retryPolicy.ExecuteAsync(
                async () =>
                    await Producer.ProduceAsync(_topic, message));

            if (result.Status != PersistenceStatus.Persisted)
                throw new Exception("Failed to produce message " + result);

            return result.TopicPartitionOffset;
        }
    }

    public void Dispose()
    {
        if (Producer != null)
        {
            Producer.Flush();
            Producer.Dispose();
        }
    }

    private Headers CreateKafkaHeaders(Dictionary<string, string>? headers = null)
    {
        if (headers == null)
        {
            headers = new Dictionary<string, string>();
        }

        headers.TryAdd(Telemetry.EventSourceHeaderKey, _serviceGroupId);
        headers.TryAdd(Telemetry.BusEventNameHeaderKey, typeof(TValue).Name);

        var result = new Headers();
        foreach (var (key, value) in headers)
        {
            result.Add(new Header(key, Encoding.UTF8.GetBytes(value)));
        }

        return result;
    }
}