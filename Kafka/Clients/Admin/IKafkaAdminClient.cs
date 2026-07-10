using Confluent.Kafka;

namespace Kafka.Clients.Admin;

public interface IKafkaAdminClient : IDisposable
{
    Task CreateDefaultTopicsAsync(params string[] topics);
    Task CreateTopicsIfNotExistsAsync(IReadOnlyCollection<Topic> topics);
    Task CreateNonExistingTopics(IReadOnlyCollection<Topic> topics, Metadata metadata);
    Task IncreasePartitionsAmount(IEnumerable<Topic> topics, Metadata metadata);
    Task DeleteTopicsIfExistsAsync(IReadOnlyCollection<Topic> topics);
}