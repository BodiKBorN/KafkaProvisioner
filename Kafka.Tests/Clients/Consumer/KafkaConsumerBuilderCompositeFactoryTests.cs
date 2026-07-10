using App.Metrics;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using Kafka.Clients.Consumer;

namespace Tech.Kafka.Tests.Clients.Consumer;

public class KafkaConsumerBuilderCompositeFactoryTests
{
    private ILogger _logger;
    private IMetrics _metrics;
    private KafkaConsumer<string, string> _kafkaConsumer;
    private KafkaConsumerBuilderCompositeFactory<string, string> _factory;

    [SetUp]
    public void Setup()
    {
        _logger = Mock.Of<ILogger>();
        _metrics = Mock.Of<IMetrics>();
        _kafkaConsumer = new KafkaConsumer<string, string>(
            "localhost:9092",
            "test-topic",
            "test-group",
            _logger,
            _metrics);
        _factory = new KafkaConsumerBuilderCompositeFactory<string, string>(_logger, _metrics, _kafkaConsumer);
    }
    
    [Test]
    public void Should_ReturnConsumer_When_Build()
    {
        // Act
        var consumer = _factory.Build();

        // Assert
        consumer.Should().NotBeNull();
    }
}