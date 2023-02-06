namespace Krimson.Persistence.Outbox;

/// <summary>
///     Allows for processing of the outbox.
///     <para />
///     This is used by the <see cref="OutboxDeliveryService{T}"/> to send messages to the specified destination topic
///     and the <see cref="OutboxCleanUpService{T}"/> to delete messages from the outbox.
/// </summary>
public interface IOutboxProcessor : IAsyncDisposable {
    /// <summary>
    ///     Processes the outbox, sending all pending messages to the specified destination topic.
    /// </summary>
    /// <returns>All processed outbox messages</returns>
    IAsyncEnumerable<OutboxMessage> ProcessOutbox(OutboxProcessingStrategy strategy, CancellationTokenSource cancellator);

    /// <summary>
    ///     Deletes all messages in the outbox that have been processed up to the specified date.
    /// </summary>
    Task<long> CleanUpOutbox(DateTimeOffset processedUpTo, CancellationToken cancellationToken);

    /// <summary>
    ///     Deletes all messages in the outbox that have been processed up to the specified retention period.
    /// </summary>
    Task<long> CleanUpOutbox(TimeSpan retentionPeriod, CancellationToken cancellationToken);

    /// <summary>
    ///     Danger! This will delete all messages in the outbox.
    /// </summary>
    Task<long> ClearOutbox(CancellationToken cancellationToken);

    /// <summary>
    ///     Loads all messages from the outbox.
    /// </summary>
    IAsyncEnumerable<OutboxMessage> LoadOutbox();

    /// <summary>
    ///     Loads all messages from the outbox that have been processed up to the specified date.
    /// </summary>
    IAsyncEnumerable<OutboxMessage> LoadOutbox(DateTimeOffset processedUpTo);
}

public enum OutboxProcessingStrategy {
    Update,
    Delete
}