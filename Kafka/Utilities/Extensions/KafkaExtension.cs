using System.Globalization;
using System.Text;
using App.Metrics;
using Confluent.Kafka;
using Kafka.Models;
using Microsoft.Extensions.Logging;

namespace Kafka.Utilities.Extensions;

public static class KafkaExtension
{
    public static LogLevel Map(this SyslogLevel kafkaLogLevel)
    {
        return kafkaLogLevel switch
        {
            SyslogLevel.Emergency => LogLevel.Critical,
            SyslogLevel.Alert => LogLevel.Critical,
            SyslogLevel.Critical => LogLevel.Critical,
            SyslogLevel.Error => LogLevel.Error,
            SyslogLevel.Warning => LogLevel.Warning,
            SyslogLevel.Notice => LogLevel.Warning,
            SyslogLevel.Info => LogLevel.Information,
            SyslogLevel.Debug => LogLevel.Debug,
            _ => LogLevel.Critical
        };
    }

    public static async Task TryExecuteWithMetricsPropagator<TKey, TValue>(
        this Func<Task> taskFunc,
        ConsumeResult<TKey, TValue> message,
        IKafkaMessagePropagator<TKey, TValue> kafkaMessagePropagator,
        string eventHandler,
        IMetrics metrics,
        Func<Message<TKey, MessageError<TValue>>, Task> selectorError)
    {
        try
        {
            using var _ = kafkaMessagePropagator.StartActivity(message);
            Telemetry.AddActivityTag("event-handler", eventHandler);
            var tags = kafkaMessagePropagator.GetMessageMetrics(message);

            using (metrics.Measure.Timer.Time(MetricsRegistry.Consume, tags))
            {
                await taskFunc();
            }
        }
        catch (Exception ex)
        {
            var serializedFailureTime = GetBytes(DateTime.UtcNow.ToString(CultureInfo.InvariantCulture));
            message.Message.Headers.Add(Telemetry.FailureTime, serializedFailureTime);
            kafkaMessagePropagator.Logger.LogError(ex, "Consume error");
            var messageError = new Message<TKey, MessageError<TValue>>()
            {
                Headers = message.Message.Headers,
                Key = message.Message.Key,
                Value = new MessageError<TValue>(message.Message.Value, ex.ToString(),  0)
            };
            await selectorError(messageError);
            var tags = kafkaMessagePropagator.GetMessageMetrics(message);
            metrics.Measure.Meter.Mark(MetricsRegistry.ConsumeExceptionCounter, tags);
        }
    }
    
    private static byte[] GetBytes(string text) => Encoding.UTF8.GetBytes(text);
}