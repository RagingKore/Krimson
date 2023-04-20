using Confluent.SchemaRegistry;
using Microsoft.Extensions.Configuration;

namespace Krimson.SchemaRegistry.Configuration;

[PublicAPI]
public record KrimsonSchemaRegistryBuilder {
    public KrimsonSchemaRegistryOptions Options { get; init; } = new();
    
    public KrimsonSchemaRegistryBuilder OverrideConfig(Action<SchemaRegistryConfig> overrideSchemaRegistryConfig) {
        Ensure.NotNull(overrideSchemaRegistryConfig, nameof(overrideSchemaRegistryConfig));

        var options = Options with { };

        overrideSchemaRegistryConfig(options.RegistryConfiguration);

        return this with { Options = options };
    }

    public KrimsonSchemaRegistryBuilder Connection(string url, string apiKey, string apiSecret, int requestTimeoutMs) {
        return OverrideConfig(
            cfg => {
                cfg.Url                        = url;
                cfg.BasicAuthUserInfo          = $"{apiKey}:{apiSecret}";
                cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
                cfg.RequestTimeoutMs           = requestTimeoutMs;
            }
        );
    }
    
    public KrimsonSchemaRegistryBuilder ReadSettings(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return Connection(
            configuration.GetValue("Krimson:SchemaRegistry:Url", Options.RegistryConfiguration.Url)!,
            configuration.GetValue("Krimson:SchemaRegistry:ApiKey", "")!,
            configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")!,
            configuration.GetValue<int>("Krimson:SchemaRegistry:RequestTimeout", Options.RegistryConfiguration.RequestTimeoutMs.Value)!
        );
    }

    public ISchemaRegistryClient Create() {
        //TODO SS: replace ensure by more specific and granular validation
        Ensure.NotNullOrWhiteSpace(Options.RegistryConfiguration.Url, nameof(Options.RegistryConfiguration.Url));

        return new CachedSchemaRegistryClient(Options.RegistryConfiguration);
    }
}