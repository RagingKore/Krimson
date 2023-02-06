using System.Collections.Concurrent;
using Krimson.Producers;

namespace Krimson.Persistence.Outbox;

public delegate Task<OutboxMessage> ProduceRequestToOutbox<in T>(ProducerRequest request, T transactionScope, CancellationToken cancellationToken = default);

/// <summary>
///     The context used by the <see cref="IOutbox{TOperationContext,TTransactionScope}"/> to push messages to the outbox.
/// </summary>
public record OutboxOperationContext<T>(T TransactionScope, CancellationToken CancellationToken) {
    internal ProduceRequestToOutbox<T> ProduceRequestToOutbox { get; init; }

    ConcurrentBag<OutboxItem> InternalItems { get; } = new();

    /// <summary>
    ///     The requests that have been persisted to the outbox along with their respective outbox message representation.
    /// </summary>
    public IEnumerable<OutboxItem> Items => InternalItems;

    /// <summary>
    ///     Pushes a single request to the outbox.
    /// </summary>
    public async Task<OutboxMessage> ProduceToOutbox(ProducerRequest request) {
        var msg = await ProduceRequestToOutbox(request, TransactionScope, CancellationToken).ConfigureAwait(false);
        InternalItems.Add(new(request, msg));
        return msg;
    }

    public Task<OutboxMessage> ProduceToOutbox(ProducerRequestBuilder builder)                             => ProduceToOutbox(builder.Create());
    public Task<OutboxMessage> ProduceToOutbox(Func<ProducerRequestBuilder, ProducerRequestBuilder> build) => ProduceToOutbox(ProducerRequest.Builder.With(build));
    public Task<OutboxMessage> ProduceToOutbox(object message, string topic)                               => ProduceToOutbox(x => x.Message(message).Topic(topic));
    public Task<OutboxMessage> ProduceToOutbox(object message, MessageKey key, string topic)               => ProduceToOutbox(x => x.Message(message).Key(key).Topic(topic));
}