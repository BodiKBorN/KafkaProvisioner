using App.Metrics;
using App.Metrics.Internal;
using Kafka.Clients;
using Kafka.Clients.Admin;
using Microsoft.Extensions.Logging;
using Tech.Kafka.IntegrationTests.Initialization.Configuration;

namespace Tech.Kafka.IntegrationTests.Initialization;

public class KafkaInitialization
{
    public string BootstrapServers { get; }
    public KafkaInitialization()
    {
        BootstrapServers = ConfigurationHandler.Instance.KafkaHost;
        KafkaAdminClient = new KafkaAdminClient(BootstrapServers);
        KafkaClientFactory = new KafkaClientFactory(BootstrapServers, Metrics.Instance, new LoggerFactory().CreateLogger("1"));
    }
    
    public KafkaAdminClient KafkaAdminClient { get; }
    
    public KafkaClientFactory KafkaClientFactory { get; }
}