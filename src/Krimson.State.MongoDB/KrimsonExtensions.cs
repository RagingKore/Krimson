// ReSharper disable CheckNamespace

using Krimson.State;
using Krimson.State.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace Krimson;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    public static KrimsonBuilder AddMongoStateStore(this KrimsonBuilder builder, string? collectionName = null) {
        builder.Services.AddKrimsonMongoStateStore(collectionName);
        return builder;
    }
}

[PublicAPI]
public static class ServiceCollectionExtensions {
    public static IServiceCollection AddKrimsonMongoStateStore(this IServiceCollection services, string? collectionName = null) => 
        services.AddSingleton<IStateStore>(ctx => new MongoStateStore(ctx.GetRequiredService<IMongoDatabase>(), collectionName));
}