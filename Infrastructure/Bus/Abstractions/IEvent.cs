namespace Tech.Social.Domain;

public interface IEvent
{
    EventPublishStrategy PublishStrategy => EventPublishStrategy.OnCompletion;
}

public enum EventPublishStrategy
{
    /// <summary>
    /// Publish immediately, ignoring any ambient transaction.
    /// </summary>
    Immediate,
    /// <summary>
    /// Queue the event and publish it only if the transaction commits.
    /// </summary>
    OnCommit,
    /// <summary>
    /// Queue the event and publish it when the transaction completes,
    /// regardless of whether it commits or rolls back.
    /// </summary>
    OnCompletion
}