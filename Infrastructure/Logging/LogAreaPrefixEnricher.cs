using Serilog.Core;
using Serilog.Events;

namespace Tech.Social.Infrastructure.Logging;

public class LogAreaPrefixEnricher : ILogEventEnricher
{
    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        if (logEvent.Properties.TryGetValue("SourceContext", out var categoryPropertyValue))
        {
            var category = categoryPropertyValue.ToString().Trim('"');

            var loggingArea = LoggerHelper.GetAreaByCategory(category);

            logEvent.AddOrUpdateProperty(new LogEventProperty("Area", new ScalarValue(loggingArea)));
        }
    }
}
