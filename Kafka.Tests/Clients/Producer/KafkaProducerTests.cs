using App.Metrics;
using AutoFixture.NUnit3;
using Kafka.Clients.Producer;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;

namespace Tech.Kafka.Tests.Clients.Producer;

public class KafkaProducerTests
{
    private Mock<ILogger> _loggerMock;
    private Mock<IMetrics> _metricsMock;
    private Mock<IProducer<string, string>> _producerMock;
    private KafkaProducer<string, string> _kafkaProducer;

    [SetUp]
    public void Setup()
    {
        _loggerMock = new Mock<ILogger>();
        _metricsMock = new Mock<IMetrics>();
        _producerMock = new Mock<IProducer<string, string>>();

        _kafkaProducer =
            new KafkaProducer<string, string>("broker", "topic", "groupId", _loggerMock.Object, _metricsMock.Object);
    }
    
    [Test, AutoData]
    public async Task Should_ProduceMessage_When_ProduceAsync(string message)
    {
        // Arrange
        var expectedTopicPartitionOffset = new TopicPartitionOffset("topic", 0, 1);

        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), CancellationToken.None))
            .ReturnsAsync(new DeliveryResult<string, string>
            {
                Status = PersistenceStatus.Persisted,
                TopicPartitionOffset = expectedTopicPartitionOffset
            });
        _kafkaProducer.Producer = _producerMock.Object;
        
        // Act
        var result = await _kafkaProducer.ProduceAsync(message);

        // Assert
        result.Should().Be(expectedTopicPartitionOffset);
    }
}