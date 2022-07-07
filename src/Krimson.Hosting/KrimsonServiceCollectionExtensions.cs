// ReSharper disable CheckNamespace

using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Extensions.DependencyInjection;

[PublicAPI]
public static class KrimsonServiceCollectionExtensions {
    public static KrimsonBuilder AddKrimson(this IServiceCollection services) => 
        new KrimsonBuilder(services).SchemaRegistry();
    
    public static IServiceCollection AddKrimson(this IServiceCollection services, Action<KrimsonBuilder> build) {
        build(new KrimsonBuilder(services).SchemaRegistry());
        return services;
    }
}