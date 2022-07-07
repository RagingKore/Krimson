using Krimson.SchemaRegistry.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Hosting;

[PublicAPI]
public static class KrimsonSchemaRegistryServiceCollectionExtensions {
    /// <summary>
    /// Registers the Schema Registry Client and automatically reads settings from configuration.
    /// </summary>
    public static IServiceCollection AddKrimsonSchemaRegistry(this IServiceCollection services, Func<IConfiguration, IServiceProvider, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) =>
        services.AddSingleton(
            serviceProvider => {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();
                
                return new KrimsonSchemaRegistryBuilder()
                    .ReadSettings(configuration)
                    .With(x => build(configuration, serviceProvider, x))
                    .Create();
            }
        );

    /// <summary>
    /// Registers the Schema Registry Client and automatically reads settings from configuration.
    /// </summary>
    public static IServiceCollection AddKrimsonSchemaRegistry(this IServiceCollection services, Func<IConfiguration, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) =>
        AddKrimsonSchemaRegistry(services, (configuration, provider, builder) => build(configuration, builder));
    
    /// <summary>
    /// Registers the Schema Registry Client and automatically reads settings from configuration.
    /// </summary>
    public static IServiceCollection AddKrimsonSchemaRegistry(this IServiceCollection services) =>
        AddKrimsonSchemaRegistry(services, (configuration, provider, builder) => builder);
}