using System.Collections.Generic;
using Tech.Kafka.Clients.Admin;

namespace Tech.Infrastructure.Options;

public class ConsumerConfiguration : IConsumerConfiguration
{
    public IReadOnlyCollection<Topic> Topics { get; set; }
    public string GroupId { get; set; }
    public bool StartFromLatest { get; set; }
}