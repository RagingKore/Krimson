namespace Krimson;

[PublicAPI]
public static class ServicesExtensions {
    public static KrimsonBuilder AddKrimson(this IServiceCollection services) => 
        new KrimsonBuilder(services).AddSchemaRegistry();
    
    public static IServiceCollection AddKrimson(this IServiceCollection services, Action<KrimsonBuilder> configure) {
        configure(AddKrimson(services));
        return services;
    }
}