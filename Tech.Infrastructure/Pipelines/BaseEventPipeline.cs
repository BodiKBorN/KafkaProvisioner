using System;
using System.Threading;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Meter;
using App.Metrics.Timer;
using Tech.Common.Extensions;
using Tech.Kafka.Clients.Consumer;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Tech.Kafka.Utilities;

namespace Tech.Social.Infrastructure.Pipelines;

public abstract class BaseEventPipeline<TKey, TValue>(
    ILogger<BaseEventPipeline<TKey, TValue>> logger,
    Func<IKafkaConsumer<TKey, TValue>> kafkaConsumer,
    IMetrics metrics)
    where TValue : class, new()
{
    protected IKafkaConsumer<TKey, TValue> _kafkaConsumer = kafkaConsumer();
    protected readonly IMetrics _metrics = metrics;
    protected bool _isStopped;
    protected IDisposable _subscription;
    protected int _messageInProcess;
    private TaskCompletionSource<bool> _restartKafkaConsumer = new();
    private readonly int _waitRestartInSeconds = 30;

    private static readonly TimerOptions Consume = new()
    {
        Name = "Kafka.Consume",
        MeasurementUnit = Unit.Calls,
        DurationUnit = TimeUnit.Seconds,
        RateUnit = TimeUnit.Seconds
    };

    protected static readonly MeterOptions ConsumeExceptionCounter = new()
    {
        Name = "Kafka Consume Exception Counter",
        MeasurementUnit = Unit.Errors
    };

    protected async Task MeasureConsumeAsync(ConsumeResult<TKey, TValue> message, Func<Task> action)
    {
        var tags = _kafkaConsumer.MessagePropagator.GetMessageMetrics(message);
        using (_metrics.Measure.Timer.Time(Consume, tags))
        {
            await action();
        }
    }

    protected async Task MeasureConsumeAsync(ConsumeResult<TKey, TValue> message, int messageCount, Func<Task> action) //TODO: must be replaced with custom metrics
    {
        var tags = _kafkaConsumer.MessagePropagator.GetMessageMetrics(message);
        var startTimestamp = _metrics.Clock.Nanoseconds;

        await action();

        var elapsed = _metrics.Clock.Nanoseconds - startTimestamp;
        var elapsedPerMessage = elapsed / Math.Max(messageCount, 1);
        var timer = _metrics.Provider.Timer.Instance(Consume, tags);

        for (var i = 0; i < messageCount; i++)
        {
            timer.Record(elapsedPerMessage, TimeUnit.Nanoseconds);
        }
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

        _ = TaskMethods.CreateLongRunning(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await _restartKafkaConsumer.Task;

                logger.LogWarning("Current consumer start disposing!");
                
                _subscription?.Dispose();
                _kafkaConsumer?.Dispose();
                
                logger.LogWarning("Current consumer has been disposed!");
                
                await Task.Delay(TimeSpan.FromSeconds(_waitRestartInSeconds), cancellationToken);

                try
                {
                    _kafkaConsumer = kafkaConsumer();
                    _restartKafkaConsumer = new TaskCompletionSource<bool>();
                    
                    logger.LogWarning("New consumer starting!");
                    
                    TryActionSource();
                    
                    logger.LogWarning("New consumer has been started!");
                }
                catch (Exception ex)
                {
                    logger.LogError("Start consuming error: {@Error}", ex);
                }
            }
            
            logger.LogWarning("Background consuming task is stopping!");
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
        logger.LogError("Consume error: {@Error}", ex);
        Interlocked.Exchange(ref _messageInProcess, 0);
    }

    protected void OnCompleted()
    {
        _restartKafkaConsumer.SetResult(true);
        logger.LogInformation("Completed consuming");
        Interlocked.Exchange(ref _messageInProcess, 0);
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