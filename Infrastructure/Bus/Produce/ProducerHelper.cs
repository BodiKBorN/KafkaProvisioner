using System.Diagnostics;
using System.Net;
using Infrastructure.Options;
using Confluent.Kafka;

namespace Tech.Social.Infrastructure.Bus;

public static class ProducerHelper
{
    public static ProducerConfig BuildProducerConfig(this KafkaHostOptions options)
    {
        return new ProducerConfig
        {
            BootstrapServers = options.Host,
            ClientId = Dns.GetHostName(),
            Acks = Acks.Leader,
            QueueBufferingMaxKbytes = 4096
        };
    }
}