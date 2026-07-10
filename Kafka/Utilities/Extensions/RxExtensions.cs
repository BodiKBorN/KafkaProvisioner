using System.Reactive.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Kafka.Utilities.Extensions;

public static class RxExtensions
{
    public static IObservable<TResult> SelectAsync<T, TResult>(this IObservable<T> source, Func<T, Task<TResult>> selector, int degreeOfParallelism = 1)
    {
        var semaphore = new SemaphoreSlim(degreeOfParallelism);

        return source
            .Do(_ => semaphore.Wait())
            .Select(
                async x =>
                {
                    try
                    {
                        return await selector(x);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                })
            .Concat();
    }
}