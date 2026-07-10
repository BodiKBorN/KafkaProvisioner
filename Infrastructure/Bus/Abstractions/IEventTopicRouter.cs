using System;
using System.Collections.Generic;
using Tech.Kafka.Clients.Admin;

namespace Infrastructure.Bus.Abstractions;

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