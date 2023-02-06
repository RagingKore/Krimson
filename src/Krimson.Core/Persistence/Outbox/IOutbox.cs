using Krimson.Producers;

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
    Task<OutboxMessage[]> WithTransaction(Func<TOperationContext, Task> operation, Func<CancellationToken, Task<TTransactionScope>> transactionScopeFactory, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Executes the specified operation within a transaction.
    /// </summary>
    Task<OutboxMessage[]> WithTransaction(Func<TOperationContext, Task> operation, CancellationToken cancellationToken = default);
}