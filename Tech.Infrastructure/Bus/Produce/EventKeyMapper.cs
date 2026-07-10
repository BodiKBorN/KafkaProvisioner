using System;
using System.Collections.Generic;
using Tech.Social.Domain;
using Tech.Social.Infrastructure.Bus.Abstractions;

namespace Tech.Social.Infrastructure.Bus.Produce;

internal class EventKeyMapper : EventKeyMapperBase
{
    public EventKeyMapper() : base(new Dictionary<Type, Func<IEvent, string>>
        {
            // {typeof(ReconsiderDynamicSegmentEvent), e => ((ReconsiderDynamicSegmentEvent) e).ClientId.ToString()},

           
        })
    { }
}