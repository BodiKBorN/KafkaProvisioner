using App.Metrics;
using Infrastructure.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using System;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Clients;
using Kafka.Clients.Producer;
using Kafka.Models;
using Kafka.Utilities.Extensions;
using Topic = Kafka.Clients.Admin.Topic;

namespace Infrastructure.Pipelines;

public class EventPipeline : BaseEventPipeline<string, JObject>, IHostedService
{
    private readonly IKafkaProducer<string, MessageError<JObject>> _kafkaProducer;
    private readonly Topic _topic;
    private readonly IServiceScopeFactory _scopeFactory;

    public EventPipeline(
        Topic topic,
        string consumerGroup,
        IServiceProvider serviceProvider,
        IKafkaClientFactory kafkaClientFactory) : base(
        serviceProvider.GetRequiredService<ILogger<EventPipeline>>(),
        () => kafkaClientFactory.GetConsumer<string, JObject>(topic?.Name, consumerGroup),
        serviceProvider.GetRequiredService<IMetrics>())
    {
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
        _scopeFactory = serviceProvider.GetRequiredService<IServiceScopeFactory>();

        var appGroupConfigurationOptions =
            serviceProvider.GetRequiredService<IOptions<AppGroupConfigurationOptions>>();

        if (_topic.DeadLetterTopic != null)
        {
            _kafkaProducer = kafkaClientFactory.GetProducer<string, MessageError<JObject>>(
                _topic.DeadLetterTopic.Name,
                appGroupConfigurationOptions.Value.ServiceGroupId);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        StartInternalBackground(() =>
        {
            _subscription = _kafkaConsumer
                .Subscribe()
                .TakeWhile(_ => !_isStopped)
                .Do(_ => Interlocked.Increment(ref _messageInProcess))
                .SelectAsync(async message =>
                {
                    using var scope = _scopeFactory.CreateScope();

                    var eventHandlerResult = scope.ServiceProvider
                        .GetRequiredService<IMessageDeserialize>()
                        .GetEventHandlerFromMessage(message.Message, scope, cancellationToken);

                    if (!eventHandlerResult.IsSuccess)
                    {
                        return message;
                    }

                    await eventHandlerResult.EventHandler.TryExecuteWithMetricsPropagator(
                        message,
                        _kafkaConsumer.MessagePropagator,
                        eventHandlerResult.HandlerType,
                        _metrics,
                        async m =>
                        {
                            if (_kafkaProducer == null)
                            {
                                return;
                            }

                            await _kafkaProducer.ProduceAsync(m);
                        });

                    return message;
                })
                .Subscribe(OnNext, OnError, OnCompleted);
        }, cancellationToken);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await StopInternalAsync(cancellationToken);
    }
}