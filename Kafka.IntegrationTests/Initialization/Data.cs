using Kafka.Clients.Admin;

namespace Tech.Kafka.IntegrationTests.Initialization;

public class Data
{
    public static Dictionary<int, string> CreateMessage()
        => Enumerable.Range(0, 200).ToDictionary(k => k, _ => Guid.NewGuid().ToString());
    
    public static Topic CreateTopic(bool withDeadLetterTopic = true)
    {
        var deadLetterTopic = withDeadLetterTopic ? CreateTopic(false) : null;
        return new Topic(Guid.NewGuid().ToString().Substring(0, 6), deadLetterTopic, PartitionAmount.Low, TopicRetentionPeriod.FewMinutes, ReplicaFactor.Minimal);
    }
}