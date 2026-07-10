using App.Metrics;
using Infrastructure.Utilities;
using Microsoft.Extensions.Logging;
using System;

namespace Tech.Social.Infrastructure.Logging;

public class MetricLoggerProvider : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName)
    {
        var metrics = ServiceLocator.GetInstance<IMetrics>();
        return new MetricLogger(metrics);
    }

    public void Dispose()
    {
        // Clean up any resources used by the logger provider
    }

    private class MetricLogger : ILogger
    {
        private readonly IMetrics _metrics;

        public MetricLogger(IMetrics metrics)
        {
            _metrics = metrics;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            return default;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if(logLevel == LogLevel.Error)
            {
                _metrics?.AddError(exception);
            }
        }
    }
}
