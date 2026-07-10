using App.Metrics;
using App.Metrics.Meter;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System.Diagnostics;
using Kafka.Clients.Consumer;
using Kafka.Models;
using Kafka.Utilities;
using Kafka.Utilities.Extensions;

namespace Tech.Kafka.Tests.Clients.Consumer;

public class KafkaConsumerTests
{
    private ILogger<KafkaConsumer<string, string>> _logger;
    private IMetrics _metrics;
    private readonly string bootstrapServers = "localhost:9092";
    private readonly string topic = "test-topic";
    private readonly string groupId = "test-group";

    [SetUp]
    public void Setup()
    {
        _logger = new Mock<ILogger<KafkaConsumer<string, string>>>().Object;
        _metrics = new Mock<IMetrics>().Object;
    }

    [Test]
    public async Task Exception_InvokesSelectorError_WithMessageError()
    {
        // Arrange
        const string key = "k1";
        var value = new JObject { ["Value"] = "Test" };
        var headers = new Headers();

        var consume = new ConsumeResult<string, JObject>
        {
            Message = new Message<string, JObject>
            {
                Key = key,
                Value = value,
                Headers = headers
            }
        };

        var loggerMock = new Mock<ILogger>();
        var propagatorMock = new Mock<IKafkaMessagePropagator<string, JObject>>();
        propagatorMock.Setup(p => p.StartActivity(It.IsAny<ConsumeResult<string, JObject>>()))
                .Returns(new Activity("DummyActivity"));
        propagatorMock.Setup(p => p.GetMessageMetrics(It.IsAny<ConsumeResult<string, JObject>>()))
                      .Returns(default(App.Metrics.MetricTags));
        propagatorMock.SetupGet(p => p.Logger).Returns(loggerMock.Object);

        var metricsMock = new Mock<IMetrics>(MockBehavior.Loose) { DefaultValue = DefaultValue.Mock };

        Message<string, MessageError<JObject>>? captured = null;

        var innerException = new NullReferenceException();
        var ex = new Exception("Foo", innerException);
        var boom = () => Task.FromException(ex);

        // Act
        await boom.TryExecuteWithMetricsPropagator(
            consume,
            propagatorMock.Object,
            "AutoClaimEventHandler",
            metricsMock.Object,
            m =>
            {
                captured = m;
                return Task.CompletedTask;
            });

        // Assert
        captured.Should().NotBeNull();
        captured?.Key.Should().Be(key);
        captured?.Headers.Should().BeSameAs(headers);
        captured?.Value.Error.Should().Be(ex.ToString());
        captured?.Value.RetryCount.Should().Be(0);

        Console.WriteLine(ex.ToString());

        metricsMock.Verify(m => m.Measure.Meter.Mark(
                It.IsAny<MeterOptions>(),
                It.IsAny<App.Metrics.MetricTags>()),
            Times.Once);
    }

    [Test]
    public async Task Success_DoesNotInvokeSelectorError()
    {
        // Arrange
        const string key = "k1";
        var value = new JObject { ["Value"] = "Test" };
        var headers = new Headers();

        var consume = new ConsumeResult<string, JObject>
        {
            Message = new Message<string, JObject>
            {
                Key = key,
                Value = value,
                Headers = headers
            }
        };

        var loggerMock = new Mock<ILogger>();
        var propagatorMock = new Mock<IKafkaMessagePropagator<string, JObject>>();
        propagatorMock.Setup(p =>
                p.StartActivity(It.IsAny<ConsumeResult<string, JObject>>()))
            .Returns(new Activity("DummyActivity"));

        propagatorMock.Setup(p => p.GetMessageMetrics(It.IsAny<ConsumeResult<string, JObject>>()))
                        .Returns(default(App.Metrics.MetricTags));
        propagatorMock.SetupGet(p => p.Logger).Returns(loggerMock.Object);

        var metricsMock = new Mock<IMetrics>(MockBehavior.Loose) { DefaultValue = DefaultValue.Mock };

        var selectorCalled = false;
        var ok = () => Task.CompletedTask;

        // Act
        await ok.TryExecuteWithMetricsPropagator(
            consume,
            propagatorMock.Object,
            "AutoClaimEventHandler",
            metricsMock.Object,
            m =>
            {
                selectorCalled = true;
                return Task.CompletedTask;
            });

        // Assert
        selectorCalled.Should().BeFalse();

        metricsMock.Verify(m => m.Measure.Meter.Mark(
                It.IsAny<MeterOptions>(),
                It.IsAny<App.Metrics.MetricTags>()),
            Times.Never);
    }

    [Test]
    public void Should_InitializeProperties_WithValidArguments()
    {
        // Act
        var consumer = new KafkaConsumer<string, string>(
            bootstrapServers,
            topic,
            groupId,
            _logger,
            _metrics);

        // Assert
        consumer.Should().NotBeNull();
        consumer.MessagePropagator.Should().NotBeNull();
        consumer.Consumer.Should().BeNull();
        consumer.Configuration.Should().NotBeNull();
        consumer.KeyDeserializer.Should().NotBeNull();
        consumer.ValueDeserializer.Should().NotBeNull();
    }

    [Test]
    public void Should_ApplyConfiguration_BuildConsumer_WithConfigureAction()
    {
        // Arrange
        Action<ConsumerConfig> configureAction = config =>
        {
            config.EnableAutoOffsetStore = true;
            config.SessionTimeoutMs = 5000;
        };

        var consumer = new KafkaConsumer<string, string>(
            bootstrapServers,
            topic,
            groupId,
            _logger,
            _metrics);

        // Act
        consumer.BuildConsumer(configureAction);

        // Assert
        consumer.Configuration.EnableAutoOffsetStore.Should().BeTrue();
        consumer.Configuration.SessionTimeoutMs.Should().Be(5000);
    }

    [Test]
    public void Should_SubscribeAndReturnObservable_When_ConsumerNotSubscribed()
    {
        // Arrange
        int offsetShift = 1;

        var consumer = new KafkaConsumer<string, string>(
            bootstrapServers,
            topic,
            groupId,
            _logger,
            _metrics);

        // Act
        consumer.BuildConsumer();
        var observable = consumer.Subscribe();

        // Assert
        observable.Should().NotBeNull();
    }
}