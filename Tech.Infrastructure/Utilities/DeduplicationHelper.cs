using System;
using System.Threading.Tasks;
using Tech.Infrastructure.Caching;

namespace Tech.Infrastructure.Utilities;

public static class DeduplicationHelper
{
    private const int CacheValidDays = 60;
    
    private static string CreateKey(object key)
        => $"f:{key}";
    
    public static Task ExecuteAsync<THash, TKey>(
        THash hashModel,
        Func<Task> func,
        TKey key,
        ICacheClient redisClient)
        where TKey : struct
        => ExecuteAsync(hashModel, func, CreateKey(key), redisClient);
    
    private static async Task ExecuteAsync<THash>(
        THash hashModel,
        Func<Task> func,
        string key,
        ICacheClient redisClient)
    {
        if (hashModel is null)
            throw new ArgumentNullException(nameof(hashModel));
        
        var currentHash = hashModel.GetHashCode();
        var prevHash = await redisClient.GetAsync<int>(key);
        
        if (prevHash.HasValue && prevHash.Value == currentHash)
            return;
        
        await func();
        await redisClient.StoreAsync(key, currentHash, TimeSpan.FromDays(CacheValidDays));
    }
}