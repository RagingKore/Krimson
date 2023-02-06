using Confluent.SchemaRegistry;
using Krimson.SchemaRegistry.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class SchemaRegistryServicesExtensions {
    /// <summary>
    /// Registers the Schema Registry Client and automatically reads settings from configuration.
    /// </summary>
    public static IServiceCollection AddKrimsonSchemaRegistry(this IServiceCollection services, Func<IServiceProvider, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) =>
        services.AddSingleton(
            serviceProvider => {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();
                
                return new KrimsonSchemaRegistryBuilder()
                    .ReadSettings(configuration)
                    .With(x => build(serviceProvider, x))
                    .Create();
            }
        );

    /// <summary>
    /// Registers the Schema Registry Client and automatically reads settings from configuration.
    /// </summary>
    public static IServiceCollection AddKrimsonSchemaRegistry(this IServiceCollection services) =>
        AddKrimsonSchemaRegistry(services, (_, builder) => builder);

    /// <summary>
    /// Registers the provided Schema Registry Client.
    /// </summary>
    public static IServiceCollection AddKrimsonSchemaRegistry(this IServiceCollection services, ISchemaRegistryClient schemaRegistryClient) =>
        services.AddSingleton(schemaRegistryClient);
}