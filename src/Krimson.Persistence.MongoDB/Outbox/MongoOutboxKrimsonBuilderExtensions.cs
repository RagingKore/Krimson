using Krimson.Persistence.Outbox;
using Polly;

namespace Krimson;

[PublicAPI]
public static class MongoOutboxKrimsonBuilderExtensions {
    public static KrimsonBuilder AddMongoOutbox(this KrimsonBuilder builder, string clientId, AsyncPolicy? outboxErrorHandlingPolicy = null) {
        builder.Services.AddKrimsonMongoOutbox(clientId, outboxErrorHandlingPolicy);
        return builder;
    }

    public static KrimsonBuilder AddMongoOutboxProcessor(this KrimsonBuilder builder, AsyncPolicy? outboxErrorHandlingPolicy = null) {
        builder.Services.AddKrimsonMongoOutboxProcessor(outboxErrorHandlingPolicy);
        return builder;
    }

    public static KrimsonBuilder AddMongoOutboxDeliveryService(this KrimsonBuilder builder, OutboxProcessingStrategy strategy, TimeSpan? backoffTime = null) {
        builder.Services.AddKrimsonMongoOutboxDeliveryService(strategy, backoffTime);
        return builder;
    }

    public static KrimsonBuilder AddMongoOutboxCleanUpService(this KrimsonBuilder builder, TimeSpan? retentionPeriod, TimeSpan? backoffTime = null) {
        builder.Services.AddKrimsonMongoOutboxCleanUpService(retentionPeriod, backoffTime);
        return builder;
    }

    /// <summary>
    ///     Adds the MongoDB transactional outbox system allowing the client to handle all aspects of the outbox, including processing and cleanup.
    /// </summary>
    public static KrimsonBuilder AddMongoOutboxSystem(this KrimsonBuilder builder, MongoOutboxSystemOptions options) {
        builder.AddMongoOutbox(options.ClientId, options.OutboxErrorHandlingPolicy);
        builder.AddMongoOutboxProcessor(options.OutboxErrorHandlingPolicy);

        builder.AddMongoOutboxDeliveryService(options.ProcessingStrategy, options.DeliveryBackoffTime);

        if (options.ProcessingStrategy == OutboxProcessingStrategy.Update) {
            builder.AddMongoOutboxCleanUpService(options.RetentionPeriod, options.CleanUpBackoffTime);
        }

        return builder;
    }

    /// <summary>
    ///     Adds the MongoDB transactional outbox system allowing the client to handle all aspects of the outbox, including processing and cleanup.
    /// </summary>
    public static KrimsonBuilder AddMongoOutboxSystem(this KrimsonBuilder builder, string clientId, AsyncPolicy? outboxErrorHandlingPolicy = null) =>
        builder.AddMongoOutboxSystem(
            new() {
                ClientId                  = clientId,
                OutboxErrorHandlingPolicy = outboxErrorHandlingPolicy
            }
        );
}

public record MongoOutboxSystemOptions {
    public string                   ClientId                  { get; init; }
    public AsyncPolicy?             OutboxErrorHandlingPolicy { get; init; } = null;
    public TimeSpan                 DeliveryBackoffTime       { get; init; } = TimeSpan.FromSeconds(5);
    public OutboxProcessingStrategy ProcessingStrategy        { get; init; } = OutboxProcessingStrategy.Delete;
    public TimeSpan                 CleanUpBackoffTime        { get; init; } = TimeSpan.FromHours(12);
    public TimeSpan                 RetentionPeriod           { get; init; } = TimeSpan.FromHours(24);
}