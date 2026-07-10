using Confluent.Kafka;
using Infrastructure.Options;
using System.Net;

namespace Infrastructure.Bus.Produce;

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