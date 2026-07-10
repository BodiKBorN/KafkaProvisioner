#nullable enable
using Infrastructure.Bus.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

namespace Infrastructure.Bus.Produce;

/// <summary>
/// A decorator that wraps an <see cref="IEventPublisher"/> to add transaction-aware event buffering capabilities.
/// </summary>
/// <remarks>
/// This class intercepts events published within an ambient <see cref="TransactionScope"/>. 
/// Depending on the event's <see cref="EventPublishStrategy"/>:
/// <list type="bullet">
/// <item>
    /// <description>Events marked as <see cref="EventPublishStrategy.OnCommit"/> are deferred and only dispatched if the ambient transaction commits successfully.</description>
/// </item>
/// <item>
/// <description>Events marked as <see cref="EventPublishStrategy.OnCompletion"/> are deferred until the transaction completes and sent regardless of its status.</description>
/// </item>
/// </list>
/// Uses a thread-safe, static <see cref="ConcurrentDictionary{TKey, TValue}"/> to track buffers across async flow boundaries.
/// </remarks>
internal sealed class TransactionBufferedEventPublisherDecorator(
    IEventPublisher innerPublisher,
    ILogger<TransactionBufferedEventPublisherDecorator> logger)
    : IEventPublisher
{
    private readonly IEventPublisher _innerPublisher = innerPublisher ?? throw new ArgumentNullException(nameof(innerPublisher));
    private readonly ILogger<TransactionBufferedEventPublisherDecorator> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    
    // Maps active transactions to their specific event queues
    private static readonly ConcurrentDictionary<Transaction, ConcurrentQueue<IEvent>> TransactionBuffers = new();

    public Task PublishEventAsync(IEvent busEvent)
    {
        ArgumentNullException.ThrowIfNull(busEvent);

        if (TryBufferEvent(busEvent))
            return Task.CompletedTask;

        return _innerPublisher.PublishEventAsync(busEvent);
    }

    public Task PublishNonKeyedEventAsync(IEvent busEvent)
    {
        ArgumentNullException.ThrowIfNull(busEvent);

        if (TryBufferEvent(busEvent))
            return Task.CompletedTask;

        return _innerPublisher.PublishNonKeyedEventAsync(busEvent);
    }

    public Task PublishEventsAsync(IEnumerable<IEvent> busEvents)
    {
        ArgumentNullException.ThrowIfNull(busEvents);

        if (Transaction.Current == null)
            return _innerPublisher.PublishEventsAsync(busEvents);

        var notBufferableEvents = busEvents
            .Where(busEvent => !TryBufferEvent(busEvent))
            .ToList();
        
        return notBufferableEvents.Count > 0
            ? _innerPublisher.PublishEventsAsync(notBufferableEvents)
            : Task.CompletedTask;
    }

    public Task PublishErrorAsync(IEvent busEvent, string message, int retryCount = 0)
    {
        // Dead Letter Queue / Error events bypass the transaction buffer completely.
        // If the business transaction fails, you still want to know about infrastructure errors immediately.
        return _innerPublisher.PublishErrorAsync(busEvent, message, retryCount);
    }

    private bool TryBufferEvent(IEvent busEvent)
    {
        if (busEvent.PublishStrategy == EventPublishStrategy.Immediate)
            return false;
        
        var currentTx = Transaction.Current;
        if (currentTx == null)
            return false;

        var buffer = TransactionBuffers.GetOrAdd(currentTx, tx =>
        {
            tx.TransactionCompleted += HandleTransactionCompleted;
            return new ConcurrentQueue<IEvent>();
        });

        buffer.Enqueue(busEvent);
        
        return true;
    }

    private void HandleTransactionCompleted(object? sender, TransactionEventArgs e)
    {
        if (!TransactionBuffers.TryRemove(e.Transaction!, out var bufferedEvents) || bufferedEvents.IsEmpty)
            return;

        var txInfo = e.Transaction?.TransactionInformation;
        var txStatus = txInfo?.Status ?? TransactionStatus.Aborted;
        var txId = txInfo?.LocalIdentifier ?? "Unknown";

        if (txStatus == TransactionStatus.Committed)
        {
            _logger.LogInformation(
                "Transaction {TransactionId} committed successfully. Offloading {Count} events to Kafka.",
                txId, bufferedEvents.Count);

            _ = Task.Run(async () =>
            {
                try
                {
                    await _innerPublisher.PublishEventsAsync(bufferedEvents);
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex,
                        "Failed to publish {Count} buffered events to Kafka after transaction {TransactionId} committed.",
                        bufferedEvents.Count, txId);
                }
            });
            
            return;
        }
        
        // Handle Aborted, InDoubt, etc.
        var publishOnCompletionEvents = bufferedEvents.Where(x => x.PublishStrategy == EventPublishStrategy.OnCompletion).ToList();
        var discardedCount = bufferedEvents.Count - publishOnCompletionEvents.Count;

        _logger.LogWarning(
            "Transaction {TransactionId} ended with status '{Status}'. Discarded {DiscardedCount} 'OnCommit' events. Publishing {PublishCount} 'OnCompletion' events.",
            txId, txStatus, discardedCount, publishOnCompletionEvents.Count);

        if (publishOnCompletionEvents.Count <= 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                await _innerPublisher.PublishEventsAsync(publishOnCompletionEvents);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex,
                    "Failed to publish {Count} 'PublishAnyway' events to Kafka after transaction {TransactionId} failed.",
                    publishOnCompletionEvents.Count, txId);
            }
        });
    }
}