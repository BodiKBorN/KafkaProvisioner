using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tech.Kafka.Clients.Admin;

namespace Tech.Social.Infrastructure.Bus.Abstractions;

public interface IEventTopicRouter
{
    Topic GetTopic(Type eventType);
}

public abstract class EventTopicRouterBase(
    Dictionary<Type, Topic> topicMappings)
    : IEventTopicRouter
{
    public Topic GetTopic(Type eventType) =>
        topicMappings.TryGetValue(eventType, out var topic) ? topic : default;
}