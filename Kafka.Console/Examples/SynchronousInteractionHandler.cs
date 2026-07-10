using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Kafka.Clients;
using Kafka.Clients.Producer;
using Confluent.Kafka;

namespace Kafka.Console.Examples;

public class SynchronousInteractionHandler
{
    private readonly IObservable<ConsumeResult<string, string>> _successStream;
    private readonly IObservable<ConsumeResult<string, string>> _errorStream;
    private readonly IKafkaProducer<string, string> _betPlaceProducer;

    public SynchronousInteractionHandler(IKafkaClientFactory kafkaClientFactory, IKafkaProducer<string, string> produce)
    {
        _successStream = kafkaClientFactory.GetConsumer<string, string>("someTopic1", "assign", enableAutoCommit: false)
            .Assign(Offset.End);
        _errorStream = kafkaClientFactory.GetConsumer<string, string>("someTopic2", "assign", enableAutoCommit: false)
            .Assign(Offset.End);

        _betPlaceProducer = produce;
    }

    public async Task<string> Execute(string id, string message)
    {
        var responseBetTask = _successStream.Where(p => p.Message.Key == id)
            .Select(c => c.Message.Value)
            .Amb(
                _errorStream.Where(p => p.Message.Key == id)
                    .Select(c => c.Message.Value))
            .FirstAsync()
            .Timeout(TimeSpan.FromSeconds(30))
            .ToTask();
        
        await _betPlaceProducer.ProduceAsync(id, message); //service B and i waut service B consum in someTopic1 or someTopic2
        return await responseBetTask; // in someTopic1 or someTopic2
    }
}