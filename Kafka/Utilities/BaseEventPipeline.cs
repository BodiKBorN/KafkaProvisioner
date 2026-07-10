using Confluent.Kafka;
using Kafka.Clients.Admin;
using Kafka.Clients.Consumer;
using Microsoft.Extensions.Logging;

namespace Kafka.Utilities;

public abstract class BaseEventPipeline<TKey, TValue>
{
    private readonly IKafkaAdminClient _kafkaAdminClient;
    private readonly ILogger<BaseEventPipeline<TKey, TValue>> _logger;
    protected IKafkaConsumer<TKey, TValue> _kafkaConsumer;
    protected bool _isStopped;
    protected IDisposable _subscription;
    protected int _messageInProcess;
    private TaskCompletionSource<bool> _restartKafkaConsumer = new ();
    private readonly int _waitRestartInSeconds = 30;
    private readonly Func<IKafkaConsumer<TKey, TValue>> _kafkaConsumerFunc;

    public BaseEventPipeline(IKafkaAdminClient kafkaAdminClient,
        ILogger<BaseEventPipeline<TKey, TValue>> logger,
        Func<IKafkaConsumer<TKey, TValue>> kafkaConsumer)
    {
        _kafkaAdminClient = kafkaAdminClient;
        _logger = logger;
        _kafkaConsumer = kafkaConsumer();
        _kafkaConsumerFunc = kafkaConsumer;
    }

    protected void StartInternalBackground(Action actionSource, CancellationToken cancellationToken)
    {
        void TryActionSource()
        {
            try
            {
                actionSource();
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
        }

        TryActionSource();

        TaskMethods.CreateLongRunning(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await _restartKafkaConsumer.Task;

                _logger.LogWarning("Current consumer start disposing!");

                _subscription?.Dispose();
                _kafkaConsumer?.Dispose();
                
                _logger.LogWarning("Current consumer has been disposed!");

                await Task.Delay(TimeSpan.FromSeconds(_waitRestartInSeconds), cancellationToken);
                    
                try
                {
                    _kafkaConsumer = _kafkaConsumerFunc();
                    _restartKafkaConsumer = new TaskCompletionSource<bool>();

                    _logger.LogWarning("New consumer starting!");

                    TryActionSource();

                    _logger.LogWarning("New consumer has been started!");
                }
                catch (Exception ex)
                {
                    _logger.LogError("Start consuming error: {@Error}", ex);
                }
            }
        }, cancellationToken);
    }

    protected void OnManyNext(params ConsumeResult<TKey, TValue>[] result)
    {
        _kafkaConsumer.StoreOffsets(result);
        Interlocked.Decrement(ref _messageInProcess);
    }
    
    protected void OnNext(ConsumeResult<TKey, TValue> result)
    {
        OnManyNext(result);
    }
    
    protected void OnError(Exception ex)
    {
        _restartKafkaConsumer.SetResult(true);
        _logger.LogError("Consume error: {@Error}", ex);
        Interlocked.Exchange(ref _messageInProcess, 0);
    }

    protected void OnCompleted()
    {
        _restartKafkaConsumer.SetResult(true);
        _logger.LogInformation("Completed consuming");
        Interlocked.Exchange(ref _messageInProcess, 0);
    }

    protected async Task CreateTopicAsync(Topic topic, bool isDev)
    {
        try
        {
            if (isDev)
            {
                topic = new Topic(
                    topic.Name,
                    topic.DeadLetterTopic,
                    PartitionAmount.Low,
                    topic.RetentionPeriod,
                    ReplicaFactor.Minimal);
            }

            await _kafkaAdminClient.CreateTopicsIfNotExistsAsync(new[] { topic });
        }
        catch(Exception e)
        {
            _logger.LogError("{@Error}", e);
        }
    }
    
    protected async Task StopInternalAsync(CancellationToken cancellationToken)
    {
        _isStopped = true;

        while (_messageInProcess > 0)
        {
            await Task.Delay(1000, cancellationToken);
        }

        _subscription?.Dispose();
    }
}


//TODO use from Tech.Base
public static class TaskMethods
{
    public static async Task CreateLongRunning(Func<Task> asyncDelegate, CancellationToken stoppingToken = default)
    {
        await Task.Factory.StartNew(
            async () => await asyncDelegate(),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Current).Unwrap();
    }
}