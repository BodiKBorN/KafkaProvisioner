using System;
using System.Collections.Generic;
using System.Linq;

namespace Infrastructure.Bus.Abstractions;

public interface ITraceableEvent : IEvent
{
    DateTime PublishedAt { get; set; }
    EventTrace[] Traces { get; set; }
}

public class EventTrace
{
    public string CorrelationId { get; set; }
    public string Path { get; set; }
    public Dictionary<string, string> Context { get; set; } = new Dictionary<string, string>();
}

public static class TraceableEventHelper
{
    public static EventTrace[] CalculateTraces(
        ITraceableEvent[] parallelEvents, string processingStage, KeyValuePair<string, string>[] additionalContext = null) =>
        parallelEvents
        .Where(pt => pt.Traces != null)
        .SelectMany(pt => pt.Traces)
        .Select(t => ExtendTrace(t, processingStage, additionalContext))
        .DistinctBy(t => t.CorrelationId)
        .ToArray();
    
    public static EventTrace[] CalculateTraces(
        ITraceableEvent traceableEvent, string processingStage, KeyValuePair<string, string>[] additionalContext = null)
    {
        if (traceableEvent.Traces == null)
            return [];
        
        return traceableEvent
            .Traces
            .Select(t => ExtendTrace(t, processingStage, additionalContext))
            .DistinctBy(t => t.CorrelationId)
            .ToArray();
    }

    public static EventTrace? FindMatchingTrace(
        this EventTrace[] eventTraces,
        string[] processingStages) =>
        eventTraces.FirstOrDefault(t => t.ContainsOrderedStages(processingStages));

    public static bool ContainsOrderedStages(this EventTrace trace, string[] processingStages)
    {
        int lastPosition = 0;

        foreach (string processingStage in processingStages)
        {
            var foundIndex = trace.Path.IndexOf(processingStage, lastPosition);
            if (foundIndex == -1)
                return false;

            lastPosition = foundIndex + processingStage.Length;
        }

        return true;
    }

    private static EventTrace ExtendTrace(EventTrace trace, string processingStage, KeyValuePair<string, string>[] additionalContext)
    {
        var context = trace.Context == null ?
            [] :
            new Dictionary<string, string>(trace.Context);

        if (additionalContext != null)
        {
            foreach (var pair in additionalContext)
            {
                context[pair.Key] = pair.Value;
            }
        }

        return new()
        {
            CorrelationId = trace.CorrelationId,
            Path = $"{trace.Path}, {processingStage}",
            Context = context
        };
    }
}

public static class ProcessingStages
{
    public const string ClientLoggedInAndDynamicSegmentCalculationRequested = "Client logged in and dynamic segment calculation was requested";

    public const string ClientRegistered = "Client registered";

    public const string DynamicSegmentCalculationRequested = "Dynamic segment calculation requested";

    public const string DynamicSegmentCalculationFinished = "Dynamic segment calculation finished";

    public static class ContextKeys
    {
        public const string RegistrationPromocode = "RegistrationPromocode";
    }
}