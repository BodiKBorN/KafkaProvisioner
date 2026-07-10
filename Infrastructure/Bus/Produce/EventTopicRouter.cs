using Infrastructure.Bus.Abstractions;
using System;
using System.Collections.Generic;
using Topic = Kafka.Clients.Admin.Topic;

namespace Infrastructure.Bus.Produce;

internal class EventTopicRouter(ICorePlatformTopics topicsRegistry) : EventTopicRouterBase(
        new Dictionary<Type, Topic>
            {
                // {typeof(ReconsiderDynamicSegmentEvent), topicsRegistry.ReconsiderDynamicSegmentTopic},
                
            })
{
}