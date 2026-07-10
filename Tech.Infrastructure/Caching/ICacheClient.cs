using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tech.Infrastructure.Caching;

public interface ICacheClient
{
    string Stats();

    ISubscriber GetSubscriber();

    Task<bool> IsHealthyAsync();

    bool Store<T>(string key, T newValue, TimeSpan validFor);

    Task<bool> StoreAsync<T>(string key, T newValue, TimeSpan validFor, bool useTag = true);

    bool MultiStore<T>(KeyValuePair<string, T>[] newValues, TimeSpan validFor);

    Task<bool> MultiStoreAsync<T>(KeyValuePair<string, T>[] newValues, TimeSpan validFor);

    /// <summary>
    /// Stores multiple key-value pairs in Redis using automatic batching to reduce
    /// client-side timeouts and pipeline pressure.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the values to be serialized and stored.
    /// </typeparam>
    /// <param name="newValues">
    /// An array of key-value pairs to store.
    /// </param>
    /// <param name="validFor">
    /// The expiration time to apply to each stored key.
    /// </param>
    /// <param name="batchSize">
    /// The maximum number of keys written per batch. Defaults to <c>1000</c>.
    /// Use a lower value when storing large payloads to avoid overflowing the Redis client pipeline or hitting memory limits.
    /// </param>
    /// <returns>
    /// <c>true</c> if all key-value pairs were successfully stored; otherwise, <c>false</c>.
    /// </returns>
    /// <remarks>
    /// If the number of items is less than or equal to <paramref name="batchSize"/>, all values are stored in a single operation.
    /// When the batch size is exceeded, the input is automatically split into smaller batches and written sequentially.
    /// </remarks>
    Task<bool> MultiStoreWithAutoBatchingAsync<T>(KeyValuePair<string, T>[] newValues, TimeSpan validFor, int batchSize = 1000);

    Task<bool> TouchAsync(string key, TimeSpan validFor);

    bool TryGet<T>(string key, out T value);

    Task<(bool, T)> TryGetAsync<T>(string key);

    CacheItemWrapper<T> Get<T>(string key);

    Task<CacheItemWrapper<T>> GetAsync<T>(string key, bool useTag = true);

    IDictionary<string, CacheItemWrapper<T>> MultiGet<T>(ICollection<string> keys);

    Task<IDictionary<string, CacheItemWrapper<T>>> MultiGetAsync<T>(ICollection<string> keys);

    /// <summary>
    /// Retrieves multiple cache entries from Redis using automatic batching
    /// to reduce large payloads, client-side timeouts, and async queue pressure.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the cached value contained in <see cref="CacheItemWrapper{T}"/>.
    /// </typeparam>
    /// <param name="keys">
    /// The collection of cache keys to retrieve.
    /// </param>
    /// <param name="batchSize">
    /// The maximum number of keys retrieved per batch. Defaults to <c>1000</c>.
    /// Use a lower value when cached entries have large serialized payloads to avoid overflowing the Redis client pipeline or hitting memory limits.
    /// </param>
    /// <returns>
    /// A dictionary mapping cache keys to their corresponding
    /// <see cref="CacheItemWrapper{T}"/> values.
    /// Returns an empty dictionary if <paramref name="keys"/> is null or empty.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Behavior:
    /// </para>
    /// <list type="bullet">
    /// <item>
    /// <description>
    /// If the number of keys is less than or equal to <paramref name="batchSize"/>, a single Redis command is executed.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// If the number of keys exceeds <paramref name="batchSize"/>, the keys are split into smaller batches and retrieved sequentially
    /// to prevent excessive payload sizes, Redis client-side timeouts, and async queue pressure.
    /// </description>
    /// </item>
    /// </list>
    /// </remarks>
    Task<IDictionary<string, CacheItemWrapper<T>>> MultiGetWithAutoBatchingAsync<T>(ICollection<string> keys, int batchSize = 1000);

    /// <summary>
    /// Attempts to retrieve a cached value by the specified key, retrying the operation if the first attempt fails.
    /// </summary>
    /// <typeparam name="T">The expected type of the cached value.</typeparam>
    /// <param name="key">The cache key to retrieve the value for.</param>
    /// <param name="value">When this method returns, contains the retrieved value if found; otherwise, the default value for type <typeparamref name="T"/>.</param>
    /// <param name="retryCount">The number of times to retry the operation if the initial attempt fails. Default is 1.</param>
    /// <returns><c>True</c> if the value was found in the cache during any attempt; otherwise, <c>False</c>.</returns>
    bool TryGetWithRetry<T>(string key, out T value, int retryCount = 1);

    void Remove(string key);

    Task RemoveAsync(string key, bool useTag = true);

    /// <summary>
    /// Removes a cache entry using the exact key provided, without applying the normalization rules used by the standard cache operations.
    /// </summary>
    /// <param name="key">The raw Redis key to remove, exactly as it exists in the cache.</param>
    /// <remarks>
    /// Use this method only for deleting keys created by external systems. For keys created by this cache client, prefer
    /// <see cref="RemoveAsync(string)"/> to ensure consistency with the normalization process.
    /// </remarks>
    Task RemoveRawAsync(string key);

    Task RemoveMultiAsync(ICollection<string> keys);

    Task FlushAllAsync();

    bool Increment(string key, ulong defaultValue, ulong increment);

    Task<bool> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan validFor);

    int? GetIncrementalValue(string key);

    Task<bool> LockTakeAsync(string key, string value, TimeSpan validFor);

    Task<bool> LockExtendAsync(string key, string value, TimeSpan validFor);

    Task<bool> LockReleaseAsync(string key, string value);
}