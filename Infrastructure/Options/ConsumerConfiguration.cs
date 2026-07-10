using System.Collections.Generic;
using Kafka.Clients.Admin;

namespace Infrastructure.Options;

public class ConsumerConfiguration : IConsumerConfiguration
{
    public IReadOnlyCollection<Topic> Topics { get; set; }
    public string GroupId { get; set; }
    public bool StartFromLatest { get; set; }
}