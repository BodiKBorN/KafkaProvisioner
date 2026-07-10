using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tech.Kafka.Clients.Admin;
using Tech.Social.Infrastructure.Bus.Abstractions;

namespace Tech.Social.Infrastructure.Bus.ProducerInvokers;

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