using Kafka.Clients.Admin;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Moq;
using NUnit.Framework;

namespace Tech.Kafka.Tests.Clients.Admin;

public class KafkaAdminClientTests
{
    private KafkaAdminClient _kafkaAdminClient;
    private Mock<IAdminClient> _adminClientMock;

    [SetUp]
    public void Setup()
    {
        _adminClientMock = new Mock<IAdminClient>();
        _kafkaAdminClient = new KafkaAdminClient("bootstrapServers");
        SetInternalClient(_kafkaAdminClient, _adminClientMock.Object);
    }

    private static void SetInternalClient(KafkaAdminClient kafkaAdminClient, IAdminClient adminClient)
    {
        var internalClientField = typeof(KafkaAdminClient)
            .GetField("_client", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        internalClientField.SetValue(kafkaAdminClient, adminClient);
    }
    
    [Test]
    public async Task Should_CreateDefaultTopics_CreateDefaultTopicsAsync()
    {
        // Arrange
        var topics = new[] { "topic1", "topic2" };

        // Act
        await _kafkaAdminClient.CreateDefaultTopicsAsync(topics);

        // Assert
        _adminClientMock.Verify(c => c.GetMetadata(TimeSpan.FromSeconds(30)), Times.Once);
        _adminClientMock.Verify(c => c.CreateTopicsAsync(It.Is<IEnumerable<TopicSpecification>>(specs =>
            specs.Count() == topics.Length &&
            specs.All(s => topics.Contains(s.Name) && s.NumPartitions == 1 && s.ReplicationFactor == 3)), null), Times.Once);
    }
    
     [Test]
    public async Task Should_CreateNonExistingTopicsAndIncreasePartitions_CreateTopicsIfNotExistsAsync()
    {
        // Arrange
        var topics = new List<Topic>
        {
            new ("topic1", null, PartitionAmount.Minimal, TopicRetentionPeriod.FewDays, ReplicaFactor.Minimal),
            new ("topic2", null, PartitionAmount.Low, TopicRetentionPeriod.FewDays, ReplicaFactor.Minimal),
        };

        var topicMetadatas = new List<TopicMetadata>()
        {
            new("existingTopic1", new List<PartitionMetadata>()
            {
                new(0, 0, new[] { 1 }, new[] { 1 }, null),
                new(1, 0, new[] { 1 }, new[] { 1 }, null)
            }, null),
            new("existingTopic2", new List<PartitionMetadata>()
            {
                new(0, 0, new[] { 1 }, new[] { 1 }, null)
            }, null)
        };

        var metadata = new Metadata(new List<BrokerMetadata>(), topicMetadatas, 1, "1");

        _adminClientMock
            .Setup(c => c.GetMetadata(TimeSpan.FromSeconds(30)))
            .Returns(metadata);

        // Act
        await _kafkaAdminClient.CreateTopicsIfNotExistsAsync(topics);

        // Assert
        _adminClientMock.Verify(c => c.GetMetadata(TimeSpan.FromSeconds(30)), Times.Once);
        _adminClientMock.Verify(c => c.CreateTopicsAsync(It.Is<IEnumerable<TopicSpecification>>(specs =>
            specs.Count() == 2 &&
            specs.Any(s => s.Name == "topic1" && s.NumPartitions == (int) PartitionAmount.Minimal && s.ReplicationFactor == (int) ReplicaFactor.Minimal) &&
            specs.Any(s => s.Name == "topic2" && s.NumPartitions == (int) PartitionAmount.Low && s.ReplicationFactor == (int) ReplicaFactor.Minimal)), null), Times.Once);
    }
    
    [Test]
    public async Task DeleteTopicsIfExistsAsync_ShouldDeleteExistingTopics()
    {
        // Arrange
        var topics = new List<Topic>
        {
            new ("topic1", null, PartitionAmount.Minimal, TopicRetentionPeriod.FewDays, ReplicaFactor.Minimal),
            new ("topic2", null, PartitionAmount.Minimal, TopicRetentionPeriod.FewDays, ReplicaFactor.Minimal),

        };

        var topicMetadatas = new List<TopicMetadata>()
        {
            new("topic1", new List<PartitionMetadata>(), null),
            new("topic2", new List<PartitionMetadata>(), null),
            new("existingTopic1", new List<PartitionMetadata>(), null),
            new("existingTopic2", new List<PartitionMetadata>(), null)
        };
        var metadata = new Metadata(new List<BrokerMetadata>(), topicMetadatas, 1, "1");


        _adminClientMock
            .Setup(c => c.GetMetadata(TimeSpan.FromSeconds(30)))
            .Returns(metadata);

        // Act
        await _kafkaAdminClient.DeleteTopicsIfExistsAsync(topics);

        // Assert
        _adminClientMock.Verify(c => c.GetMetadata(TimeSpan.FromSeconds(30)), Times.Once);
        _adminClientMock.Verify(c => c.DeleteTopicsAsync(It.Is<IEnumerable<string>>(topicsToDelete =>
            topicsToDelete.Count() == 2 &&
            topicsToDelete.Contains("topic1") &&
            topicsToDelete.Contains("topic2")), null), Times.Once);
    }
}