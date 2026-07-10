namespace Kafka.Models;

public record MessageError<TValue>(TValue Value, string Error, int RetryCount);