// See https://aka.ms/new-console-template for more information

using App.Metrics;
using Confluent.Kafka;
using Kafka.Clients;
using Kafka.Clients.Admin;
using Kafka.Console.Examples;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Kafka.Console;

// починить автотесты
// перепроверить с новым выставлением офсета
internal class Program
{
    static async Task<int> Main(string[] args)
    {
        System.Console.WriteLine("Start");

        var bootstrapServers = "localhost:29092,localhost:29093,localhost:29094";
        var kafkaAdminClient = new KafkaAdminClient(bootstrapServers);

        var nonStrictTopic = new Topic("local.tech.kafka.fc1.insync1.dirtElectTrue", null, PartitionAmount.Low, TimeSpan.FromDays(10), ReplicaFactor.Minimal);
        var strictTopic = new Topic("local.tech.kafka.rf3.insync2.dirtElectFalse", null, PartitionAmount.Low, TimeSpan.FromDays(10), ReplicaFactor.Default);

        var topicList = new List<Topic>
        {
            //nonStrictTopic,
            strictTopic
        }.AsReadOnly();

        await kafkaAdminClient.CreateTopicsIfNotExistsAsync(topicList);

        System.Console.WriteLine("Choose the mode: 1 = producer, 2 = consumer");
        var input = System.Console.ReadLine();

        if (input == "1")
        {
            await RunProducerAsync(bootstrapServers, strictTopic.Name);
            System.Console.WriteLine("To exit press Enter");
            System.Console.ReadLine();
            return 0;
        }

        else if (input == "2")
        {
            var sub = await RunConsumerAsync(bootstrapServers, strictTopic.Name);
            System.Console.WriteLine("To exit press Enter");
            System.Console.ReadLine();

            await sub.StopAsync(CancellationToken.None);
            return 0;
        }

        else
        {
            System.Console.WriteLine("Invalid input");
            return 1;
        }
    }

    private static async Task RunProducerAsync(string bootstrapServers, string topic)
    {
        var kafkaClient = new KafkaClientFactory(bootstrapServers, Metrics.Instance, new LoggerFactory().CreateLogger("1"));
        var kafkaProducer = kafkaClient.GetProducer<object, object>(topic, "Console");

        var stopwatch = Stopwatch.StartNew();

        var counter = 0L;
        var total = 10_000_000;
        var delayMs = 10;

        for (long i = 0; i < total; i++)
        {
            var id = Interlocked.Increment(ref counter);

            //await 
            kafkaProducer.ProduceAsync(
            new { key = id.ToString() },
            new SimpleMessage
            {
                Id = id.ToString(),
                DateTime = DateTime.UtcNow
            }
        );

            if (id % 100_000 == 0)
                System.Console.WriteLine($"Sent {id}");

            if (delayMs > 0)
                await Task.Delay(delayMs);
        }

        stopwatch.Stop();

        //System.Console.WriteLine("Elapsed time for produce: {0} s", stopwatch.ElapsedMilliseconds / 1000);
    }

    // нужно репродюснуть то, что происходит в платформе - запустить в хостед сервисе
    private static async Task<SimpleSubscribePipeline> RunConsumerAsync(string bootstrapServers, string topic)
    {
        var loggerFactory = LoggerFactory.Create(x =>
        {
            x.AddConsole(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "hh:mm:ss ";
                options.LogToStandardErrorThreshold = LogLevel.Information;
            });
        });

        var logger = loggerFactory.CreateLogger<Program>();


        var kafkaClient = new KafkaClientFactory(bootstrapServers, Metrics.Instance, logger);
        var kafkaProducer = kafkaClient.GetProducer<object, object>(topic, "Console");

        // subscribe
        var sub = new SimpleSubscribePipeline(kafkaClient.GetConsumer<string, SimpleMessage>(
            topic: topic,
            consumeGroup: "console-test1-ENV",
            enableAutoCommit: true,
            autoOffsetReset: AutoOffsetReset.Latest,
            enableAutoOffsetStore: false
            )
         );
        await sub.StartAsync(CancellationToken.None);

        System.Console.WriteLine("Subscribed!");

        return sub;
    }
}
