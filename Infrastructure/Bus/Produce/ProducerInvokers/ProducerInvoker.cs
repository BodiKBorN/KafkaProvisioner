using Infrastructure.Bus.Abstractions;
using Infrastructure.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Clients;
using Kafka.Clients.Producer;

namespace Infrastructure.Bus.Produce.ProducerInvokers;

internal class ProducerInvoker<TEvent> : IProducerInvoker
    where TEvent : class, IEvent
{
    private readonly IKafkaProducer<string, TEvent> _producer;

    public ProducerInvoker(
        IOptions<AppGroupConfigurationOptions> appGroupConfigurationOptions,
        ITopicResolver topicResolver,
        IKafkaClientFactory kafkaClientFactory)
    {
        var eventType = typeof(TEvent);

        if (eventType.IsAbstract)
            throw new InvalidOperationException($"Cannot create producer invoker for abstract type {eventType.Name}");

        var topic = topicResolver.GetTopic(eventType);

        _producer = kafkaClientFactory.GetProducer<string, TEvent>(
            topic.Name, appGroupConfigurationOptions.Value.ServiceGroupId);
    }

    public Task ProduceAsync(string key, IEvent message, Dictionary<string, string> headers)
    {
        if (message is not TEvent typedMessage)
        {
            throw new ArgumentException($"Invoker for {typeof(TEvent).Name} cannot process {message.GetType().Name}");
        }

        return _producer.ProduceAsync(key, typedMessage, headers);
    }
}

internal class ProducerInvokerRegistry(IServiceProvider serviceProvider)
{
    private readonly ConcurrentDictionary<Type, IProducerInvoker> _cache = new();

    public IProducerInvoker GetInvoker(IEvent message)
    {
        var eventType = message.GetType();

        return _cache.GetOrAdd(eventType, t =>
        {
            var concreteType = typeof(ProducerInvoker<>).MakeGenericType(t);
            return (IProducerInvoker) serviceProvider.GetRequiredService(concreteType);
        });
    }
}

internal interface IProducerInvoker
{
    Task ProduceAsync(string key, IEvent message, Dictionary<string, string> headers);
}