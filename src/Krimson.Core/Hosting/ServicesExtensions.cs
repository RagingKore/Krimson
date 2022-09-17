using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class ServicesExtensions {
    public static KrimsonBuilder AddKrimson(this IServiceCollection services, string? clientId = null) => 
        new KrimsonBuilder(services, clientId).AddSchemaRegistry();
    
    public static IServiceCollection AddKrimson(this IServiceCollection services, Action<KrimsonBuilder> configure) {
        configure(AddKrimson(services));
        return services;
    }
}