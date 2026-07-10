using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Tech.Social.Infrastructure.Pipelines.Assign;

public interface IAssignEventHandler<TKey, TValue>
{
    Task HandleAsync(ConsumeResult<TKey, TValue> message, CancellationToken cancellationToken);
}