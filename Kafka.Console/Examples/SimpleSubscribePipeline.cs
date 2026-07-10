using System.Reactive.Linq;
using Kafka.Clients.Consumer;
using Kafka.Utilities.Extensions;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace Kafka.Console.Examples;

public class SimpleMessage
{
    public string Id { get; init; }
    public DateTime DateTime { get; init; }
}

public class SimpleSubscribePipeline : IHostedService
{
    private readonly IKafkaConsumer<string, SimpleMessage> _kafkaConsumer;
    private bool _isStopped;
    private IDisposable _subscription;
    private int _messageInProcess;

    public SimpleSubscribePipeline(IKafkaConsumer<string, SimpleMessage> kafkaConsumer)
    {
        _kafkaConsumer = kafkaConsumer;
        
    }
    public Task StartAsync(CancellationToken cancellationToken)
    {
        int _messageProcesssed = 0;
        _subscription = _kafkaConsumer
            .Subscribe()
            .TakeWhile(_ => !_isStopped)
            .SelectAsync(async message =>
            {
                Interlocked.Increment(ref _messageInProcess);
                try
                {
                    await Task.Delay(1000); // simulate processing
                    System.Console.WriteLine($"Consumed = {Interlocked.Increment(ref _messageProcesssed)}; Key = ${message.Message.Key?.ToString()}; Partition = {message.Partition.Value};  Offset = {message.Offset.Value}");
                }
                catch
                {
                    //spec behavior
                }

                return message.TopicPartitionOffset;
            }, 100) // by default 1 we need this parallelism when dont matter query
            .Subscribe(
                offset =>
                {
                    _kafkaConsumer.StoreOffsets(offset);
                    Interlocked.Decrement(ref _messageInProcess);
                },
                ex =>
                {
                    //log error
                    Interlocked.Exchange(ref _messageInProcess, 0);
                },
                () =>
                {
                    //log information in completed
                    Interlocked.Exchange(ref _messageInProcess, 0);
                });

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _isStopped = true;

        while (_messageInProcess > 0)
        {
            await Task.Delay(1000);
        }

        _subscription?.Dispose();
    }
}