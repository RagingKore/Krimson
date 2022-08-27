using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace Krimson.State.MongoDB;

[PublicAPI]
public static class ServicesExtensions {
    public static IServiceCollection AddMongoStateStore(this IServiceCollection services, string? collectionName = null) => 
        services.AddSingleton<IStateStore>(ctx => new MongoStateStore(ctx.GetRequiredService<IMongoDatabase>(), collectionName));
}