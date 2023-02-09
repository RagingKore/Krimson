// ReSharper disable CheckNamespace

using Krimson.Persistence.MongoDB.Outbox;
using Krimson.Persistence.Outbox;
using Krimson.Producers;
using Krimson.Serializers;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using Polly;

namespace Krimson;

[PublicAPI]
public static class MongoOutboxWireUpExtensions {

    /// <summary>
    ///     Adds the Mongo Outbox to the service collection.
    ///     This is the outbox that is used to store messages that are to be sent to Kafka.
    ///     The outbox is used to ensure that messages are sent to Kafka in the correct order.
    ///     The outbox is also used to ensure that messages are sent to Kafka even if the application crashes.
    /// </summary>
    /// <param name="services"></param>
    /// <param name="clientId"></param>
    /// <param name="outboxErrorHandlingPolicy"></param>
    /// <returns></returns>
    public static IServiceCollection AddKrimsonMongoOutbox(this IServiceCollection services, string clientId, AsyncPolicy? outboxErrorHandlingPolicy = null) {
        services.AddSingleton<MongoOutbox>(
            ctx => new(
                clientId,
                ctx.GetRequiredService<IMongoDatabase>(),
                ctx.GetRequiredService<IDynamicSerializer>(),
                outboxErrorHandlingPolicy
            )
        );

        services.AddSingleton<IOutbox<IClientSessionHandle>>(ctx => ctx.GetRequiredService<MongoOutbox>());

        return services;
    }

    public static IServiceCollection AddKrimsonMongoOutboxProcessor(this IServiceCollection services, AsyncPolicy? outboxErrorHandlingPolicy = null) {
        services.AddSingleton<MongoOutboxProcessor>(
            ctx => new(
                ctx.GetRequiredService<IMongoDatabase>(),
                ctx.GetRequiredService<KrimsonProducer>(),
                outboxErrorHandlingPolicy
            )
        );

        services.AddSingleton<IOutboxProcessor>(ctx => ctx.GetRequiredService<MongoOutboxProcessor>());

        return services;
    }

    public static IServiceCollection AddKrimsonMongoOutboxDeliveryService(this IServiceCollection services, OutboxProcessingStrategy strategy, TimeSpan? backoffTime = null) {
        services.AddHostedService<OutboxDeliveryService<MongoOutboxProcessor>>(
            ctx => new(
                ctx.GetRequiredService<MongoOutboxProcessor>(),
                backoffTime ?? TimeSpan.FromSeconds(5),
                strategy
            )
        );

        return services;
    }

    public static IServiceCollection AddKrimsonMongoOutboxCleanUpService(this IServiceCollection services, TimeSpan? retentionPeriod, TimeSpan? backoffTime = null) {
        services.AddHostedService<OutboxCleanUpService<MongoOutboxProcessor>>(
            ctx => new(
                ctx.GetRequiredService<MongoOutboxProcessor>(),
                retentionPeriod ?? TimeSpan.FromHours(24),
                backoffTime ?? TimeSpan.FromHours(12)
            )
        );

        return services;
    }
}