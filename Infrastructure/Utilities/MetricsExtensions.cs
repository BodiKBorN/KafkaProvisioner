using App.Metrics;
using App.Metrics.Meter;
using System;

namespace Infrastructure.Utilities;

public static class MetricsExtensions
{
    public const string SourceTag = "source";
    public static MetricTags CreateSourceTag(this string source) => new(SourceTag, source);

    private static readonly MeterOptions ExceptionsCounter = new()
    {
        Name = "Exceptions",
        MeasurementUnit = Unit.Errors,
    };

    private static readonly MeterOptions UnhandledExceptionsCounter = new()
    {
        Name = "Unhandled Exceptions",
        MeasurementUnit = Unit.Errors,
    };

    public static void AddError(this IMetrics metrics, Exception exception)
    {
        var tags = new MetricTags("exception_type", exception?.GetType().Name ?? "no_exception");

        metrics.Measure.Meter.Mark(ExceptionsCounter, tags);
    }

    public static void AddUnhandledError(this IMetrics metrics, Exception exception)
    {
        var tags = new MetricTags("exception_type", exception?.GetType().Name ?? "no_exception");

        metrics.Measure.Meter.Mark(UnhandledExceptionsCounter, tags);
    }
}