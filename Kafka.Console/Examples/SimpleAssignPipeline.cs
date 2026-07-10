using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json.Linq;
using System;
using System.Reactive.Linq;
using Kafka.Clients.Consumer;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace Kafka.Console.Examples;

public class SimpleAssignPipeline : IHostedService
{
    private readonly IKafkaConsumer<string, SimpleMessage> _kafkaConsumer;
    private bool _isStopped;
    private IDisposable _subscription;
    
    public SimpleAssignPipeline(IKafkaConsumer<string, SimpleMessage> kafkaConsumer)
    {
        _kafkaConsumer = kafkaConsumer;
        
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _subscription = _kafkaConsumer
            .Assign(Offset.Beginning)
            .TakeWhile(_ => !_isStopped)
            .Do(async message =>
            {
                try
                {
                    var r = new Random();
                    await Task.Delay(r.Next(1_000, 10_000));
                    System.Console.WriteLine(message.Offset);
                }
                catch
                {
                    //spec behavior
                }
            })
            .Subscribe(
                offset =>
                {

                },
                ex =>
                {
                    System.Console.WriteLine(ex);
                },
                () =>
                {
                });

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}