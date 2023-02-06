using Krimson.Producers;

namespace Krimson.Persistence.Outbox;

[PublicAPI]
public static class OutboxExtensions {
    public static Task<OutboxMessage> ProduceToOutbox<T>(this IOutbox<T> outbox, T transactionScope, ProducerRequestBuilder builder) =>
        outbox.ProduceToOutbox(builder.Create(), transactionScope);

    public static Task<OutboxMessage> ProduceToOutbox<T>(this IOutbox<T> outbox, T transactionScope, Func<ProducerRequestBuilder, ProducerRequestBuilder> build) =>
        outbox.ProduceToOutbox(transactionScope, ProducerRequest.Builder.With(build));

    public static Task<OutboxMessage> ProduceToOutbox<T>(this IOutbox<T> outbox, T transactionScope, object message, string topic) =>
        outbox.ProduceToOutbox(transactionScope, x => x.Message(message).Topic(topic));

    public static Task<OutboxMessage> ProduceToOutbox<T>(this IOutbox<T> outbox, T transactionScope, object message, MessageKey key, string topic) =>
        outbox.ProduceToOutbox(transactionScope, x => x.Message(message).Key(key).Topic(topic));
}