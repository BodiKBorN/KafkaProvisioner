using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Tech.Infrastructure.Options;
using Tech.Kafka.Clients.Producer;
using Tech.Kafka.Clients;
using Tech.Kafka.Models;
using Tech.Kafka.Utilities;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Tech.Social.Domain;
using Tech.Social.Domain.Extensions;

namespace Tech.Social.Infrastructure.Bus.ProducerInvokers;

internal class ErrorProducerInvoker<TEvent> : IErrorProducerInvoker
    where TEvent : class, IEvent
{
    private readonly IKafkaProducer<string, MessageError<TEvent>> _producer;

    public ErrorProducerInvoker(
        IOptions<AppGroupConfigurationOptions> appGroupConfigurationOptions,
        ITopicResolver topicResolver,
        IKafkaClientFactory kafkaClientFactory)
    {
        var eventType = typeof(TEvent);

        if (eventType.IsAbstract)
            throw new InvalidOperationException($"Cannot create error invoker for abstract type {eventType.Name}");
        
        var topic = topicResolver.GetTopic(eventType);

        if (topic.DeadLetterTopic == null || string.IsNullOrWhiteSpace(topic.DeadLetterTopic.Name))
            throw new InvalidOperationException(
                $"Configuration Error: No DeadLetter Kafka topic found for event type '{eventType.Name}'. " +
                "Check your IEventTopicRouter registrations.");

        _producer = kafkaClientFactory.GetProducer<string, MessageError<TEvent>>(
            topic.DeadLetterTopic.Name, appGroupConfigurationOptions.Value.ServiceGroupId);
    }

    public Task ProduceAsync(string key, IEvent sourceEvent, string errorMessage, int retryCount, Dictionary<string, string> headers)
    {
        if (sourceEvent is not TEvent typedSourceEvent)
        {
            throw new ArgumentException($"Error Producer Invoker for {typeof(TEvent).Name} cannot process {sourceEvent.GetType().Name}");
        }

        var eventToSend = new MessageError<TEvent>(typedSourceEvent, errorMessage, retryCount);

        headers ??= new Dictionary<string, string>();
        headers.TryAdd(Telemetry.BusEventNameHeaderKey, typeof(TEvent).GetBusEventName());

        return _producer.ProduceAsync(messageKey: key, message: eventToSend, header: headers);
    }
}

internal class ErrorProducerInvokerRegistry(IServiceProvider serviceProvider)
{
    private readonly ConcurrentDictionary<Type, IErrorProducerInvoker> _cache = new();

    public IErrorProducerInvoker GetInvoker(IEvent message)
    {
        var eventType = message.GetType();

        return _cache.GetOrAdd(eventType, t =>
        {
            var concreteType = typeof(ErrorProducerInvoker<>).MakeGenericType(t);
            return (IErrorProducerInvoker) serviceProvider.GetRequiredService(concreteType);
        });
    }
}

internal interface IErrorProducerInvoker
{
    Task ProduceAsync(string key, IEvent sourceEvent, string errorMessage, int retryCount, Dictionary<string, string> headers);
}