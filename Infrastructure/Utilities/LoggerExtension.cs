using App.Metrics;
using Microsoft.Extensions.Logging;
using System;

namespace Infrastructure.Utilities;

public static class LoggerExtension
{
    public static void LogErrorWithMetrics(this ILogger logger, IMetrics metrics, Exception exception, string messageTemplate, params object[] args)
    {
        metrics.AddUnhandledError(exception);
        logger.LogError(exception, messageTemplate, args);
    }
    
    public static void LogErrorWithMetrics(this ILogger logger, IMetrics metrics, Exception exception, string messageTemplate)
    {
        metrics.AddUnhandledError(exception);
        logger.LogError(messageTemplate, exception);
    }

    public static void LogWarningWithMetrics(this ILogger logger, IMetrics metrics, Exception exception, string messageTemplate, params object[] args)
    {
        logger.LogWarning(exception, messageTemplate, args);
    }

    public static void LogInfoWithMetrics(this ILogger logger, IMetrics metrics, Exception exception, string messageTemplate, params object[] args)
    {
        logger.LogInformation(exception, messageTemplate, args);
    }

    public static void DebugUnhandledError(this ILogger logger, IMetrics metrics, Exception exception, string messageTemplate, params object[] args)
    {
        logger.LogDebug(exception, messageTemplate, args);
    }
}