using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;

namespace Infrastructure.Pipelines.Assign;

public interface IAssignEventHandler<TKey, TValue>
{
    Task HandleAsync(ConsumeResult<TKey, TValue> message, CancellationToken cancellationToken);
}