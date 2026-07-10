using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;

namespace Tech.Infrastructure.Utilities
{
    public static class LogLevelExtension
    {
        public static LogLevel MapToInternal(this SyslogLevel kafkaLogLevel)
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
                _ => throw new ArgumentOutOfRangeException(nameof(kafkaLogLevel), kafkaLogLevel, null)
            };
        }
    }
}
