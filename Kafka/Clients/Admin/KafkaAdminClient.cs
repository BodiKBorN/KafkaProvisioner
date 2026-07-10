using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Clients.Admin;

public class KafkaAdminClient : IKafkaAdminClient
{
    private readonly IAdminClient _client;
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

    public KafkaAdminClient(string bootstrapServers)
    {
        _client = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
    }

    public async Task CreateDefaultTopicsAsync(params string[] topics)
    {
        try
        {
            var metadata = _client.GetMetadata(_timeout);
            var topicSpecifications = topics.Select(e => 
                new Topic(e, null, PartitionAmount.Low, null, ReplicaFactor.Default))
            .ToList()
            .AsReadOnly();

            await CreateNonExistingTopics(topicSpecifications, metadata);
        }
        catch (CreateTopicsException e) when (e.Results.All(x => x.Error.Code is ErrorCode.TopicAlreadyExists))
        {
        }
    }

    public async Task CreateTopicsIfNotExistsAsync(IReadOnlyCollection<Topic> topics)
    {
        try
        {
            var metadata = _client.GetMetadata(_timeout);
            await CreateNonExistingTopics(topics, metadata);
            await IncreasePartitionsAmount(topics, metadata);
        }
        catch (CreateTopicsException e) when (e.Results.All(x => x.Error.Code is ErrorCode.TopicAlreadyExists))
        {
        }
    }

    public async Task CreateNonExistingTopics(IReadOnlyCollection<Topic> topics, Metadata metadata)
    {
        var existingTopics = metadata.Topics.Select(x => x.Topic);
        var topicsToCreate = Flatten(topics).Where(x => !existingTopics.Contains(x.Name)).ToList();

        if (topicsToCreate.Any())
        {
            await _client.CreateTopicsAsync(topicsToCreate.Select(x =>
                {
                    var topicSpecification = new TopicSpecification
                    {
                        Name = x.Name,
                        ReplicationFactor = (short)x.ReplicaFactor,
                        NumPartitions = (int)x.PartitionsAmount,
                        Configs = new()
                    };

                    var isConsistencyMode = topicSpecification.ReplicationFactor > 2;
                    topicSpecification.Configs.Add("min.insync.replicas", isConsistencyMode ? "2" : "1");
                    topicSpecification.Configs.Add("unclean.leader.election.enable", isConsistencyMode ? "false" : "true");


                    if (!x.RetentionPeriod.HasValue)
                        return topicSpecification;

                    var retention = (int)x.RetentionPeriod.Value.TotalMilliseconds;
                    topicSpecification.Configs.Add("retention.ms", retention.ToString());

                    return topicSpecification;
                }
            ));
        }
    }

    public async Task IncreasePartitionsAmount(IEnumerable<Topic> topics, Metadata metadata)
    {
        var targetTopicsDictionary = topics.ToDictionary(x => x.Name, x => x);
        var increasePartitionsTopic = metadata.Topics
            .Select(x => GetTopicsWithLowPartitionsAmount(targetTopicsDictionary, x))
            .Where(x => x.Item2 > 0)
            .ToList();

        if (increasePartitionsTopic.Any())
        {
            await _client.CreatePartitionsAsync(increasePartitionsTopic.Select(x => new PartitionsSpecification
            {
                Topic = x.Item1,
                IncreaseTo = x.Item2,
            }));
        }
    }

    public async Task DeleteTopicsIfExistsAsync(IReadOnlyCollection<Topic> topics)
    {
        if (!topics.Any())
        {
            return;
        }
        try
        {
            var metadata = _client.GetMetadata(TimeSpan.FromSeconds(30));

            var topicNamesToDelete = Flatten(topics)
                .Select(x => x.Name)
                .ToHashSet();

            var existedTopics = metadata.Topics.Select(x => x.Topic).ToList();
            var topicsToDelete = existedTopics.Where(topicNamesToDelete.Contains).ToList();
            if (!topicsToDelete.Any()) return;

            await _client.DeleteTopicsAsync(topicsToDelete);
        }
        catch (DeleteTopicsException e) when (e.Results.All(x => x.Error.Code is ErrorCode.NoError)) { };
    }

    public void Dispose()
    {
        _client.Dispose();
    }

    internal IEnumerable<TopicPartition> GetAllPartitions(string topic)
        => _client.GetMetadata(topic, _timeout).Topics.FirstOrDefault(t => t.Topic == topic)?.Partitions
            ?.Select(p => new TopicPartition(topic, new Partition(p.PartitionId)));

    private (string, int) GetTopicsWithLowPartitionsAmount(
        IReadOnlyDictionary<string, Topic> targetTopicsDictionary,
        TopicMetadata existingTopic)
    {
        if (!targetTopicsDictionary.ContainsKey(existingTopic.Topic))
            return (existingTopic.Topic, 0);

        var targetTopic = targetTopicsDictionary[existingTopic.Topic];
        if (targetTopic.PartitionsAmount - existingTopic.Partitions.Count > 0)
            return (existingTopic.Topic, (int)targetTopic.PartitionsAmount);
        return (existingTopic.Topic, -1);
    }

    private static IEnumerable<Topic> Flatten(IReadOnlyCollection<Topic> original)
    {
        return original.Concat(
            original
                .Where(x => !string.IsNullOrWhiteSpace(x.DeadLetterTopic?.Name))
                .Select(x => x.DeadLetterTopic)
        );
    }
}