using Microsoft.Extensions.Options;
using System.Reflection;
using Infrastructure.Options;
using Tech.Kafka.Clients.Admin;
using Tech.Social.Infrastructure.Bus;

namespace Tech.Deployment.KafkaSetup.Services;

internal class TopicsScanner
{
    private readonly List<Topic> _allTopics;
    
    public TopicsScanner(IOptions<KafkaHostOptions> kafkaOptions)
    {
        var topics = new CorePlatformTopics(kafkaOptions.Value);

        _allTopics = topics
                     .GetType()
                     .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                     .Where(p => p.PropertyType == typeof(Topic))
                     .Select(p => (Topic)p.GetValue(topics)!)
                     .ToList();
    }

    public List<Topic> GetTopics() => _allTopics;
}