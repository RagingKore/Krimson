using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.SchemaRegistry;
using Krimson.SchemaRegistry.Protobuf;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using static System.String;
using static Krimson.DefaultConfigs;

namespace Krimson.Hosting;

[PublicAPI]
public record KrimsonConfigurator {
    internal IServiceCollection Services { get; init; } = null!;
    internal KrimsonOptions     Options  { get; init; } = null!;

    public KrimsonConfigurator Connection(
        string bootstrapServers, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        Ensure.NotNullOrWhiteSpace(bootstrapServers, nameof(bootstrapServers));

        return this with {
            Options = Options with {
                Connection = new() {
                    BootstrapServers = bootstrapServers,
                    Username         = username ?? "",
                    Password         = password ?? "",
                    SecurityProtocol = protocol,
                    SaslMechanism    = mechanism
                }
            }
        };
    }

    public KrimsonConfigurator Connection(ClientConnection connection) {
        Ensure.NotNull(connection, nameof(connection));
        
        return Connection(
            connection.BootstrapServers, connection.Username, connection.Password,
            connection.SecurityProtocol, connection.SaslMechanism
        );
    }


    public KrimsonConfigurator SchemaRegistry(ISchemaRegistryClient schemaRegistryClient) {
        Ensure.NotNull(schemaRegistryClient, nameof(schemaRegistryClient));

        return this with {
            Options = Options with {
                RegistryFactory = () => schemaRegistryClient
            }
        };
    }
    
    public KrimsonConfigurator SchemaRegistry(Action<SchemaRegistryConfig> configure) {
        Ensure.NotNull(configure, nameof(configure));

        return SchemaRegistry(new CachedSchemaRegistryClient(DefaultSchemaRegistryConfig.With(configure)));
    }

    public KrimsonConfigurator SchemaRegistry(string url, string apiKey, string apiSecret) {
        Ensure.NotNullOrWhiteSpace(url, nameof(url));
        
        return SchemaRegistry(
            cfg => {
                cfg.Url                        = url;
                cfg.BasicAuthUserInfo          = !IsNullOrWhiteSpace(apiKey) && !IsNullOrWhiteSpace(apiSecret) ? $"{apiKey}:{apiSecret}" : "";
                cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
            }
        );
    }

    public KrimsonConfigurator Serializer(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        Ensure.NotNull(getSerializer, nameof(getSerializer));
            
        return this with {
            Options = Options with {
                SerializerFactory = getSerializer
            }
        };
    }

    public KrimsonConfigurator Deserializer(Func<ISchemaRegistryClient, IDynamicDeserializer> getDeserializer) {
        Ensure.NotNull(getDeserializer, nameof(getDeserializer));
        
        return this with {
            Options = Options with {
                DeserializerFactory = getDeserializer
            }
        };
    }

    public KrimsonConfigurator Configuration(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", DefaultConsumerConfig.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", DefaultConsumerConfig.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", DefaultConsumerConfig.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", DefaultConsumerConfig.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", DefaultConsumerConfig.SaslMechanism!.Value)
            )
            .SchemaRegistry(
                configuration.GetValue("Krimson:SchemaRegistry:Url", DefaultSchemaRegistryConfig.Url),
                configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
                configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            );
    }

    public KrimsonConfigurator Processor(
        int tasks,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Services.AddKrimsonProcessor(
            tasks, (ctx, bld) => bld
                .Connection(Options.Connection)
                .SchemaRegistry(Options.RegistryFactory)
                .Deserializer(Options.DeserializerFactory)
                .Serializer(Options.SerializerFactory)
                .With(x => build(ctx, x)), initialize
        );

        return this with { };
    }

    public KrimsonConfigurator Processor(
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) =>
        Processor(1, build, initialize);

    public KrimsonConfigurator Producer(Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }

    public KrimsonConfigurator Producer(Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) => 
        Producer((_, builder) => build(builder));
}

record KrimsonOptions {
    public KrimsonOptions() {
        Connection          = new();
        RegistryFactory     = () => new CachedSchemaRegistryClient(DefaultSchemaRegistryConfig);
        DeserializerFactory = registry => new ProtobufDynamicDeserializer(registry);
        SerializerFactory   = registry => new ProtobufDynamicSerializer(registry);
    }

    public ClientConnection                                  Connection          { get; init; }
    public Func<ISchemaRegistryClient>                       RegistryFactory     { get; init; }
    public Func<ISchemaRegistryClient, IDynamicDeserializer> DeserializerFactory { get; init; }
    public Func<ISchemaRegistryClient, IDynamicSerializer>   SerializerFactory   { get; init; }
}
