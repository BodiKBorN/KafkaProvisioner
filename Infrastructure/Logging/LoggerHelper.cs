using Serilog;
using Serilog.Core;
using Serilog.Events;
using Serilog.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Infrastructure.Logging;

public static class LoggerHelper
{
    private static Dictionary<LoggingArea, LoggingLevelSwitch> _areaSwitcherHolder;

    public const LogEventLevel DefaultLevel = LogEventLevel.Warning;

    static LoggerHelper()
    {
        Areas = Enum.GetValues<LoggingArea>();
    }

    private static readonly Dictionary<string, LoggingArea> CategoryAreasHolder = new()
    {
        { "Microsoft.EntityFrameworkCore.Database.Command", LoggingArea.Database },

        { "Enyim.Caching", LoggingArea.Caching },
        { "Tech.Social.Infrastructure.Cache", LoggingArea.Caching },

        { "Microsoft.AspNetCore.HttpLogging.HttpLoggingMiddleware", LoggingArea.HTTP },
        { "Tech.Social.WebSiteWebApi.Services.Http", LoggingArea.HTTP },

        { "Infrastructure.Bus", LoggingArea.Messaging },
        { "Confluent.Kafka", LoggingArea.Messaging }
    };

    public static LoggerConfiguration ConfigureLogAreas(this LoggerConfiguration loggerConfiguration, LogEventLevel defaultLogLevel)
    {
        Initialize(defaultLogLevel);

        foreach (var (category, area) in CategoryAreasHolder)
        {
            loggerConfiguration.MinimumLevel.Override(category, GetSwitcher(area));
        }

        return loggerConfiguration
            .MinimumLevel.ControlledBy(GetSwitcher(LoggingArea.Common))
            .Enrich.With(new LogAreaPrefixEnricher());
    }

    public static LoggerConfiguration AddMetricLogging(this LoggerConfiguration loggerConfiguration)
    {
        var providers = new LoggerProviderCollection();

        providers.AddProvider(new MetricLoggerProvider());

        return loggerConfiguration
            .WriteTo.Providers(providers);
    }

    public static IEnumerable<LoggingArea> Areas { get; }

    public static void Update(LoggingArea area, LogEventLevel level)
    {
        GetSwitcher(area).MinimumLevel = level;
    }

    private static void Initialize(LogEventLevel defaultLogLevel)
    {
        _areaSwitcherHolder = Areas.ToDictionary(area => area, _ => new LoggingLevelSwitch(defaultLogLevel));
    }

    private static LoggingLevelSwitch GetSwitcher(LoggingArea area)
    {
        return _areaSwitcherHolder[area];
    }

    public static LoggingArea GetAreaByCategory(string category)
    {

        return CategoryAreasHolder
            .FirstOrDefault(pair => category.StartsWith(pair.Key, StringComparison.OrdinalIgnoreCase))
                .Value;
    }
}
