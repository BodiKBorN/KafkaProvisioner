using System;

namespace Tech.Infrastructure.Caching;

[Serializable]
public readonly struct CacheItemWrapper<T>
{

    public CacheItemWrapper(bool hasValue, T value)
    { 
        HasValue = hasValue;
        Value = value; 
    }

    public T Value { get; init; }

    public bool HasValue { get; init; }
}