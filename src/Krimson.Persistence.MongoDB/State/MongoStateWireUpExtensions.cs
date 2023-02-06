// ReSharper disable CheckNamespace

using Krimson.Persistence.MongoDB.State;
using Krimson.Persistence.State;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace Krimson;

[PublicAPI]
public static class MongoStateWireUpExtensions {
    public static IServiceCollection AddKrimsonMongoStateStore(this IServiceCollection services, string? defaultCollectionName = null) {
        services.AddSingleton<IStateStore>(ctx => new MongoStateStore(ctx.GetRequiredService<IMongoDatabase>(), defaultCollectionName));
        services.AddSingleton<StateStoreFactory>(ctx => groupName => new MongoStateStore(ctx.GetRequiredService<IMongoDatabase>(), groupName));

        return services;
    }
}