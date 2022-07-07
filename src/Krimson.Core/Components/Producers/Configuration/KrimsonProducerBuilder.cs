using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.Producers.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.SchemaRegistry.Configuration;
using Microsoft.Extensions.Configuration;

namespace Krimson.Producers;

[PublicAPI]
public record KrimsonProducerBuilder {
    public KrimsonProducerOptions Options { get; init; } = new();
    
    public KrimsonProducerBuilder OverrideConfiguration(Action<ProducerConfig> configureProducer) {
        Ensure.NotNull(configureProducer, nameof(configureProducer));

        return this with {
            Options = Options with {
                Configuration = new ProducerConfig(Options.Configuration).With(configureProducer)
            }
        };
    }

    public KrimsonProducerBuilder OverrideConfiguration(ProducerConfig configuration) {
        return this with {
            Options = Options with {
                Configuration = Ensure.NotNull(configuration, nameof(configuration))
            }
        };
    }

    
    public KrimsonProducerBuilder Connection(
        string bootstrapServers, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        return OverrideConfiguration(
            cfg => {
                cfg.BootstrapServers = bootstrapServers;
                cfg.SaslUsername     = username ?? "";
                cfg.SaslPassword     = password ?? "";
                cfg.SecurityProtocol = protocol;
                cfg.SaslMechanism    = mechanism;
            }
        );
    }

    public KrimsonProducerBuilder Connection(ClientConnection connection) {
        return Connection(
            connection.BootstrapServers, connection.Username, connection.Password,
            connection.SecurityProtocol, connection.SaslMechanism
        );
    }

    public KrimsonProducerBuilder ClientId(string clientId) {
        return OverrideConfiguration(cfg => cfg.ClientId = clientId);
    }

    // public KrimsonProducerBuilder SchemaRegistry(Func<KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> buildSchemaRegistry) {
    //     var builder = buildSchemaRegistry(Options.RegistryBuilder);
    //     
    //     return this with {
    //         Options = Options with {
    //             RegistryBuilder = builder,
    //             RegistryFactory = () => builder.Create()
    //         }
    //     };
    // }

    public KrimsonProducerBuilder SchemaRegistry(ISchemaRegistryClient schemaRegistryClient) {
        Ensure.NotNull(schemaRegistryClient, nameof(schemaRegistryClient));
        
        return this with {
            Options = Options with {
                RegistryFactory = () => schemaRegistryClient
            }
        };
    }
    
    // public KrimsonProducerBuilder SchemaRegistry(string url, string apiKey = "", string apiSecret = "") {
    //     return SchemaRegistry(builder => builder.Connection(url, apiKey, apiSecret));
    // }
    
    public KrimsonProducerBuilder Serializer(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        return this with {
            Options = Options with {
                SerializerFactory = Ensure.NotNull(getSerializer, nameof(getSerializer))
            }
        };
    }

    public KrimsonProducerBuilder Topic(string? topic) {
        return this with {
            Options = Options with {
                DefaultTopic = topic
            }
        };
    }

    public KrimsonProducerBuilder Intercept(InterceptorModule interceptor, bool prepend = false) {
        Ensure.NotNull(interceptor, nameof(interceptor));

        return this with {
            Options = Options with {
                Interceptors = prepend
                    ? Options.Interceptors.Prepend(interceptor)
                    : Options.Interceptors.Append(interceptor)
            }
        };
    }
    
    public KrimsonProducerBuilder EnableDebug(bool enable = true, string? context = null) {
        return OverrideConfiguration(cfg => cfg.EnableDebug(enable, context));
    }
    
    public KrimsonProducerBuilder ReadSettings(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", Options.Configuration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", Options.Configuration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", Options.Configuration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", Options.Configuration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", Options.Configuration.SaslMechanism!.Value)
            )
            .ClientId(
                configuration.GetValue(
                    "Krimson:Output:ClientId",
                    configuration.GetValue("Krimson:ClientId", Options.Configuration.ClientId)
                )
            )
            .Topic(configuration.GetValue("Krimson:Output:Topic", ""));
            // .SchemaRegistry(builder => builder.ReadSettings(configuration));
    }
    
    public KrimsonProducer Create() {
        //TODO SS: replace ensure by more specific and granular validation
        Ensure.NotNullOrWhiteSpace(Options.Configuration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.Configuration.BootstrapServers, nameof(Options.Configuration.BootstrapServers));
        Ensure.NotNull(Options.SerializerFactory, nameof(Serializer));

        var interceptors = Options.Interceptors
            .Prepend(new KrimsonProducerLogger().WithName($"KrimsonProducer({Options.Configuration.ClientId})"))
            .Prepend(new ConfluentProducerLogger());

        return new KrimsonProducer(
            Options.Configuration,
            interceptors.Intercept,
            Options.SerializerFactory(Options.RegistryFactory()),
            Options.DefaultTopic
        );
    }
}