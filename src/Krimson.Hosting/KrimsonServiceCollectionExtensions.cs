using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class KrimsonServiceCollectionExtensions {
    public static KrimsonBuilder AddKrimson(this IServiceCollection services) => 
        new KrimsonBuilder(services).AddSchemaRegistry();
    
    public static IServiceCollection AddKrimson(this IServiceCollection services, Action<KrimsonBuilder> configure) {
        configure(new KrimsonBuilder(services).AddSchemaRegistry());
        return services;
    }
}