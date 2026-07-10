using Infrastructure.Bus.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Clients.Admin;

namespace Infrastructure.Bus.Produce.ProducerInvokers;

internal interface ITopicResolver
{
    Topic GetTopic(Type eventType);
}

internal class TopicResolver(
    IEnumerable<IEventTopicRouter> eventTopicRouters)
    : ITopicResolver
{
    public Topic GetTopic(Type eventType)
    {
        var topic = eventTopicRouters
            .Select(r => r.GetTopic(eventType))
            .Where(t => t != null)
            .LastOrDefault();

        if (topic == null || string.IsNullOrWhiteSpace(topic.Name))
            throw new InvalidOperationException(
                $"Configuration Error: No Kafka topic found for event type '{eventType.Name}'. " +
                "Check your IEventTopicRouter registrations.");

        return topic;
    }
}