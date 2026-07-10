using System;
using System.Collections.Generic;
using Tech.Social.Infrastructure.Bus.Abstractions;
using Topic = Tech.Kafka.Clients.Admin.Topic;

namespace Tech.Social.Infrastructure.Bus.Produce;

internal class EventTopicRouter(ICorePlatformTopics topicsRegistry) : EventTopicRouterBase(
        new Dictionary<Type, Topic>
            {
                // {typeof(ReconsiderDynamicSegmentEvent), topicsRegistry.ReconsiderDynamicSegmentTopic},
                
            })
{
}