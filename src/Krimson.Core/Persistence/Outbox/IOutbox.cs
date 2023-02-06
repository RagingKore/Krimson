using Krimson.Producers;
using OneOf.Types;

namespace Krimson.Persistence.Outbox;

/// <summary>
///     An outbox that ensures that messages are sent in the correct order and
///     within a transaction to Kafka even if the application crashes.
/// </summary>
public interface IOutbox<in TTransactionScope> : IAsyncDisposable {

    /// <summary>
    ///     Pushes a single request to the outbox.
    /// </summary>
    Task<OutboxMessage> ProduceToOutbox(ProducerRequest request, TTransactionScope transactionScope, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Pushes many requests to the outbox.
    /// </summary>
    Task<OutboxMessage[]> ProduceManyToOutbox(ProducerRequest[] request, TTransactionScope transactionScope, CancellationToken cancellationToken = default);
}

/// <summary>
///     An outbox that ensures that messages are sent in the correct order and
///     within a transaction to Kafka even if the application crashes.
/// </summary>
public interface IOutbox<out TOperationContext, TTransactionScope> : IOutbox<TTransactionScope> where TOperationContext : OutboxOperationContext<TTransactionScope> {
    /// <summary>
    ///     Executes the specified operation within a transaction.
    /// </summary>
    Task<TResult> WithTransaction<TResult>(Func<TOperationContext, Task<TResult>> operation, Func<CancellationToken, Task<TTransactionScope>> transactionScopeFactory, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Executes the specified operation within a transaction.
    /// </summary>
    Task<TResult> WithTransaction<TResult>(Func<TOperationContext, Task<TResult>> operation, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Executes the specified operation within a transaction and returns the result and the messages that were pushed to the outbox.
    /// </summary>
    Task<OutboxTransactionResult<TResult>> WithTransactionResult<TResult>(Func<TOperationContext, Task<TResult>> operation, Func<CancellationToken, Task<TTransactionScope>> transactionScopeFactory, CancellationToken cancellationToken);

    /// <summary>
    ///     Executes the specified operation within a transaction and returns the result and the messages that were pushed to the outbox.
    /// </summary>
    Task<OutboxTransactionResult<TResult>> WithTransactionResult<TResult>(Func<TOperationContext, Task<TResult>> operation, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Executes the specified operation within a transaction and returns the messages that were pushed to the outbox.
    /// </summary>
    Task<OutboxTransactionResult<None>> WithTransactionResult(Func<TOperationContext, Task> operation, Func<CancellationToken, Task<TTransactionScope>> transactionScopeFactory, CancellationToken cancellationToken);

    /// <summary>
    ///     Executes the specified operation within a transaction and returns the messages that were pushed to the outbox.
    /// </summary>
    Task<OutboxTransactionResult<None>> WithTransactionResult(Func<TOperationContext, Task> operation, CancellationToken cancellationToken = default);
}

public record OutboxTransactionResult<T>(T OperationResult, OutboxItem[] Items) {
    /// <summary>
    ///     The requests that have been persisted to the outbox.
    /// </summary>
    public IEnumerable<ProducerRequest> Requests => Items.Select(x => x.Request);

    /// <summary>
    ///     The messages that have been pushed to the outbox.
    /// </summary>
    public IEnumerable<OutboxMessage> OutboxMessages => Items.Select(x => x.OutboxMessage);
};

public record OutboxItem(ProducerRequest Request, OutboxMessage OutboxMessage);