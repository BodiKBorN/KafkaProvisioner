using Confluent.Kafka;
using FluentAssertions;
using Kafka.Clients.Admin;
using System.Reactive.Linq;
using Tech.Kafka.IntegrationTests.Initialization;

namespace Tech.Kafka.IntegrationTests.Tests;

public class SubscribeTests
{
    public async Task Should_Subscribe_When_AutoOffsetResetIsEarliest()
    {
        // Arrange
        var kafkaInitialization = new KafkaInitialization();
        var topicDto = Data.CreateTopic() with { PartitionsAmount = PartitionAmount.Minimal };
        var consumeGroup = Guid.NewGuid().ToString();
        var messages = Data.CreateMessage();
        await kafkaInitialization.KafkaAdminClient
            .CreateTopicsIfNotExistsAsync(new[] { topicDto });

        var kafkaProducer =
            kafkaInitialization.KafkaClientFactory.GetProducer<int, string>(topicDto.Name, "Console");

        await Task.WhenAll(messages
            .Select(message => kafkaProducer.ProduceAsync(message.Key, message.Value)));

        var consumer = kafkaInitialization
            .KafkaClientFactory
            .GetConsumer<int, string>(topicDto.Name, consumeGroup, autoOffsetReset: AutoOffsetReset.Earliest);

        // Act
        int messageInProcess = 0;
        consumer
            .Subscribe()
            .Do(p =>
            {
                if (messages[p.Message.Key] == p.Message.Value)
                {
                    messageInProcess++;
                }

                consumer.StoreOffsets(p.TopicPartitionOffset);
            })
            .Subscribe();

        async Task CheckMessageInProcess()
        {
            while (messageInProcess != messages.Count)
            {
                await Task.Delay(100);
            }
        }

        // Assert
        try
        {
            var task = CheckMessageInProcess();
            await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30)));
            task.IsCompleted.Should().BeTrue();
        }
        finally
        {
            await kafkaInitialization.KafkaAdminClient.DeleteTopicsIfExistsAsync(new[] { topicDto });
        }
    }


    public async Task Should_Subscribe_When_AutoOffsetResetIsLatest()
    {
        // Arrange
        var kafkaInitialization = new KafkaInitialization();
        var topicDto = Data.CreateTopic() with { PartitionsAmount = PartitionAmount.Minimal };
        var consumeGroup = Guid.NewGuid().ToString();
        var newMessage = (1111, "1111");
        var messages = Data.CreateMessage();
        await kafkaInitialization.KafkaAdminClient
            .CreateTopicsIfNotExistsAsync(new[] { topicDto });

        var kafkaProducer =
            kafkaInitialization.KafkaClientFactory.GetProducer<int, string>(topicDto.Name, "Console");

        await Task.WhenAll(messages
            .Select(message => kafkaProducer.ProduceAsync(message.Key, message.Value)));

        var consumer = kafkaInitialization
            .KafkaClientFactory
            .GetConsumer<int, string>(topicDto.Name, consumeGroup, autoOffsetReset: AutoOffsetReset.Latest);

        int messageInProcess = 0;
        bool IsNewMessage = false;

        // Act
        consumer
            .Subscribe()
            .Do(p =>
            {
                if (p.Message.Key == newMessage.Item1)
                {
                    IsNewMessage = true;
                }

                else if (messages[p.Message.Key] == p.Message.Value)
                {
                    messageInProcess++;
                }

                consumer.StoreOffsets(p.TopicPartitionOffset);
            })
            .Subscribe();

        await kafkaProducer.ProduceAsync(newMessage.Item1, newMessage.Item2);

        async Task CheckMessageInProcess()
        {
            while (messageInProcess != 0 && !IsNewMessage)
            {
                await Task.Delay(100);
            }
        }

        // Assert
        try
        {
            var task = CheckMessageInProcess();
            await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30)));
            task.IsCompleted.Should().BeTrue();
        }
        finally
        {
            await kafkaInitialization.KafkaAdminClient.DeleteTopicsIfExistsAsync(new[] { topicDto });
        }
    }
}