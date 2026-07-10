using Infrastructure.Bus.Abstractions;
using System;
using System.Collections.Generic;

namespace Infrastructure.Bus.Produce;

internal class EventKeyMapper : EventKeyMapperBase
{
    public EventKeyMapper() : base(new Dictionary<Type, Func<IEvent, string>>
        {
            // {typeof(ReconsiderDynamicSegmentEvent), e => ((ReconsiderDynamicSegmentEvent) e).ClientId.ToString()},

           
        })
    { }
}