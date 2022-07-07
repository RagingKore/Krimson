using Confluent.SchemaRegistry;

namespace Krimson.SchemaRegistry.Configuration;

[PublicAPI]
public record KrimsonSchemaRegistryOptions {
    public SchemaRegistryConfig RegistryConfiguration { get; init; } = DefaultConfigs.DefaultSchemaRegistryConfig;
}