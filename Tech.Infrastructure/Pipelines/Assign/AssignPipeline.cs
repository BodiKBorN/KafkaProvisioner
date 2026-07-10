using System;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using App.Metrics;
using Tech.Infrastructure.Options;
using Tech.Kafka.Clients;
using Tech.Kafka.Clients.Consumer;
using Tech.Kafka.Utilities.Extensions;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using Topic = Tech.Kafka.Clients.Admin.Topic;

namespace Tech.Social.Infrastructure.Pipelines.Assign;

public class AssignPipeline<TKey, TValue>(Topic topic,
        IMetrics metrics,
        ILogger<AssignPipeline<TKey, TValue>> logger,
        IKafkaClientFactory kafkaClientFactory,
        IOptions<AppGroupConfigurationOptions> appGroupConfigurationOptions,
        IAssignEventHandler<TKey, TValue> assignEventHandler)
    : ResilientHostedService(logger, metrics)
{
    private readonly IKafkaConsumer<TKey, TValue> _kafkaConsumer = kafkaClientFactory.GetConsumer<TKey, TValue>(
        topic.Name,
        appGroupConfigurationOptions.Value.ServiceGroupId,
        false);

    private bool _isStopped;
    private IDisposable _subscription;

    protected override Task StartImplementationAsync(CancellationToken cancellationToken)
    {
        _subscription = _kafkaConsumer
            .Assign(Offset.End)
            .TakeWhile(_ => !_isStopped)
            .SelectAsync(async message =>
            {
                try
                {
                    await assignEventHandler.HandleAsync(message, cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error during consuming {@Message}, {@Topic}", message, topic);
                }

                return message;
            })
            .Subscribe();
        
        return Task.CompletedTask;
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _isStopped = true;
        _subscription?.Dispose();

        return Task.CompletedTask;
    }
}

public class AssignBroadcastPipeline(Topic topic,
    IMetrics metrics,
    ILogger<AssignBroadcastPipeline> logger,
    IKafkaClientFactory kafkaClientFactory,
    IOptions<AppGroupConfigurationOptions> appGroupConfigurationOptions,
    IAssignEventHandler<string, JObject> assignEventHandler,
    BaseAssignEventHandler<string, JObject> baseAssignEventHandler)
    : ResilientHostedService(logger, metrics)
{
    private readonly IKafkaConsumer<string, JObject> _kafkaConsumer = kafkaClientFactory.GetConsumer<string, JObject>(
        topic.Name,
        appGroupConfigurationOptions.Value.ServiceGroupId,
        false);

    private bool _isStopped;
    private IDisposable _subscription;

    protected override async Task StartImplementationAsync(CancellationToken cancellationToken)
    {
        _subscription = _kafkaConsumer
            .Assign(Offset.End)
            .Buffer(TimeSpan.FromSeconds(1))
            .TakeWhile(_ => !_isStopped)
            .Where(p => p.Any())
            .SelectAsync(async messages =>
            {
                foreach (var message in messages
                             .GroupBy(p => (p.Message.Key, baseAssignEventHandler.GetNameEvent(p)))
                             .Select(p => p.MaxBy(m => m.Offset.Value)))
                {
                    try
                    {
                        await assignEventHandler.HandleAsync(message, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Error during consuming {@Message}, {@Topic}", message, topic);
                    }
                }

                return messages;
            })
            .Subscribe();
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _isStopped = true;
        _subscription?.Dispose();

        return Task.CompletedTask;
    }
}