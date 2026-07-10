using Confluent.Kafka;
using FluentAssertions;
using NUnit.Framework;
using Tech.Kafka.IntegrationTests.Initialization;

namespace Tech.Kafka.IntegrationTests.Tests;

public class AdminClientTests
{
    [Test]
    public async Task Should_CreateTopic()
    {
        // Arrange
        var kafkaInitialization = new KafkaInitialization();
        var client = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafkaInitialization.BootstrapServers }).Build();
        var topicDto = Data.CreateTopic();
        
        // Act
        await kafkaInitialization.KafkaAdminClient
            .CreateTopicsIfNotExistsAsync(new [] { topicDto });

        // Assert
        var topicMetadata = client.GetMetadata(topicDto.Name, TimeSpan.FromHours(1))
            .Topics.FirstOrDefault(t => t.Topic == topicDto.Name);
        topicMetadata.Topic.Should().NotBeNull();
        topicMetadata.Partitions.Count.Should().Be((int)topicDto.PartitionsAmount);

        var topicErrorMetadata = client
            .GetMetadata(topicDto.DeadLetterTopic.Name, TimeSpan.FromHours(1))
            .Topics.FirstOrDefault(t => t.Topic == topicDto.DeadLetterTopic.Name);
        topicErrorMetadata.Topic.Should().NotBeNull();
        topicErrorMetadata.Partitions.Count.Should().Be((int)topicDto.PartitionsAmount);

        await kafkaInitialization.KafkaAdminClient.DeleteTopicsIfExistsAsync(new[] { topicDto });
    }
    
    [Test]
    public async Task Should_DeleteTopic()
    {
        // Arrange
        var kafkaInitialization = new KafkaInitialization();
        var client = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafkaInitialization.BootstrapServers }).Build();
        var topicDto = Data.CreateTopic();
        await kafkaInitialization.KafkaAdminClient
            .CreateTopicsIfNotExistsAsync(new [] { topicDto });
        
        // Act
        await kafkaInitialization.KafkaAdminClient.DeleteTopicsIfExistsAsync(new[] { topicDto });

        // Assert
        var topicMetadata = client.GetMetadata(topicDto.Name, TimeSpan.FromHours(1))
            .Topics.FirstOrDefault(t => t.Topic == topicDto.Name);
        topicMetadata.Partitions.Count.Should().Be(0);
        
        var topicErrorMetadata = client
            .GetMetadata(topicDto.DeadLetterTopic.Name, TimeSpan.FromHours(1))
            .Topics.FirstOrDefault(t => t.Topic == topicDto.DeadLetterTopic.Name);
        topicErrorMetadata.Partitions.Count.Should().Be(0);
    }
    
    [Test]
    public async Task Should_IncreasePartitionsAmount()
    {
        // Arrange
        var kafkaInitialization = new KafkaInitialization();
        var client = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafkaInitialization.BootstrapServers }).Build();
        var topicDto = Data.CreateTopic();
        
        // Act
        await kafkaInitialization.KafkaAdminClient
            .CreateTopicsIfNotExistsAsync(new [] { topicDto });

        var incTopicDto = topicDto with { PartitionsAmount = topicDto.PartitionsAmount + 2 };
        await kafkaInitialization.KafkaAdminClient
            .CreateTopicsIfNotExistsAsync(new [] { incTopicDto });

        // Assert
        var topicMetadata = client.GetMetadata(topicDto.Name, TimeSpan.FromHours(1))
            .Topics.FirstOrDefault(t => t.Topic == topicDto.Name);
        topicMetadata.Topic.Should().NotBeNull();
        topicMetadata.Partitions.Count.Should().Be((int)incTopicDto.PartitionsAmount);
        
        var topicErrorMetadata = client
            .GetMetadata(topicDto.DeadLetterTopic.Name, TimeSpan.FromHours(1))
            .Topics.FirstOrDefault(t => t.Topic == topicDto.DeadLetterTopic.Name);
        topicErrorMetadata.Topic.Should().NotBeNull();
        topicErrorMetadata.Partitions.Count.Should().Be((int)incTopicDto.PartitionsAmount - 2);

        await kafkaInitialization.KafkaAdminClient.DeleteTopicsIfExistsAsync(new[] { incTopicDto });
    }
}