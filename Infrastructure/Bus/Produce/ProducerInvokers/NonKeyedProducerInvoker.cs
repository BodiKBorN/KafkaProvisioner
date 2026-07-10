using Confluent.Kafka;
using Infrastructure.Bus.Abstractions;
using Infrastructure.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Tech.Kafka.Clients;
using Tech.Kafka.Clients.Producer;

namespace Infrastructure.Bus.Produce.ProducerInvokers;

internal class NonKeyedProducerInvoker<TEvent> : INonKeyedProducerInvoker
    where TEvent : class, IEvent
{
    private readonly IKafkaProducer<Ignore, TEvent> _producer;

    public NonKeyedProducerInvoker(
        IOptions<AppGroupConfigurationOptions> appGroupConfigurationOptions,
        ITopicResolver topicResolver,
        IKafkaClientFactory kafkaClientFactory)
    {
        var eventType = typeof(TEvent);

        if (eventType.IsAbstract)
            throw new InvalidOperationException($"Cannot create non-keyed invoker for abstract type {typeof(TEvent).Name}");

        var topic = topicResolver.GetTopic(eventType);

        _producer = kafkaClientFactory.GetProducer<Ignore, TEvent>(
            topic.Name, appGroupConfigurationOptions.Value.ServiceGroupId);
    }

    public Task ProduceAsync(IEvent message, Dictionary<string, string> headers)
    {
        if (message is not TEvent typedMessage)
        {
            throw new ArgumentException($"Invoker for {typeof(TEvent).Name} cannot process {message.GetType().Name}");
        }

        return _producer.ProduceAsync(typedMessage, headers);
    }
}

internal class NonKeyedProducerInvokerRegistry(IServiceProvider serviceProvider)
{
    private readonly ConcurrentDictionary<Type, INonKeyedProducerInvoker> _cache = new();

    public INonKeyedProducerInvoker GetInvoker(IEvent message)
    {
        var eventType = message.GetType();

        return _cache.GetOrAdd(eventType, t =>
        {
            var concreteType = typeof(NonKeyedProducerInvoker<>).MakeGenericType(t);
            return (INonKeyedProducerInvoker) serviceProvider.GetRequiredService(concreteType);
        });
    }
}

internal interface INonKeyedProducerInvoker
{
    Task ProduceAsync(IEvent message, Dictionary<string, string> headers);
}