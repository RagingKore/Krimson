using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Interceptors;
using Krimson.Producers.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.SchemaRegistry.Protobuf;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Krimson.Producers;

[PublicAPI]
public record KrimsonProducerBuilder {
    public KrimsonProducerOptions Options { get; init; } = new();

    public KrimsonProducerBuilder Connection(
        string bootstrapServers, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        return OverrideProducerConfig(
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
        return OverrideProducerConfig(cfg => cfg.ClientId = clientId);
    }

    public KrimsonProducerBuilder OverrideSchemaRegistryConfig(Action<SchemaRegistryConfig> configureSchemaRegistry) {
        Ensure.NotNull(configureSchemaRegistry, nameof(configureSchemaRegistry));

        var options = Options with { };

        configureSchemaRegistry(options.RegistryConfiguration);

        return this with {
            Options = options
        };
    }

    public KrimsonProducerBuilder SchemaRegistry(string url, string apiKey, string apiSecret) {
        return OverrideSchemaRegistryConfig(
            cfg => {
                cfg.Url                        = url;
                cfg.BasicAuthUserInfo          = $"{apiKey}:{apiSecret}";
                cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
            }
        );
    }

    public KrimsonProducerBuilder SchemaRegistry(ISchemaRegistryClient schemaRegistryClient) {
        Ensure.NotNull(schemaRegistryClient, nameof(schemaRegistryClient));

        return this with {
            Options = Options with {
                RegistryFactory = () => schemaRegistryClient
            }
        };
    }

    public KrimsonProducerBuilder Serializer(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        return this with {
            Options = Options with {
                SerializerFactory = Ensure.NotNull(getSerializer, nameof(getSerializer))
            }
        };
    }

    public KrimsonProducerBuilder UseProtobuf(Action<ProtobufSerializerConfig>? configure = null) {
        return Serializer(registry => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configure?.Invoke(x))));
    }

    public KrimsonProducerBuilder Topic(string? topic) {
        return this with {
            Options = Options with {
                DefaultTopic = topic
            }
        };
    }

    public KrimsonProducerBuilder OverrideProducerConfig(Action<ProducerConfig> configureProducer) {
        Ensure.NotNull(configureProducer, nameof(configureProducer));

        return this with {
            Options = Options with {
                ProducerConfiguration = new ProducerConfig(Options.ProducerConfiguration).With(configureProducer)
            }
        };
    }

    public KrimsonProducerBuilder Configuration(ProducerConfig configuration) {
        return this with {
            Options = Options with {
                ProducerConfiguration = Ensure.NotNull(configuration, nameof(configuration))
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
    
    public KrimsonProducerBuilder LoggerFactory(ILoggerFactory loggerFactory) {
        return this with {
            Options = Options with {
                LoggerFactory = Ensure.NotNull(loggerFactory, nameof(loggerFactory))
            }
        };
    }

    public KrimsonProducerBuilder EnableDebug(bool enable = true, string? context = null) {
        return OverrideProducerConfig(cfg => cfg.EnableDebug(enable, context));
    }
    
    public KrimsonProducerBuilder ReadSettings(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", Options.ProducerConfiguration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", Options.ProducerConfiguration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", Options.ProducerConfiguration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", Options.ProducerConfiguration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", Options.ProducerConfiguration.SaslMechanism!.Value)
            )
            .SchemaRegistry(
                configuration.GetValue("Krimson:SchemaRegistry:Url", Options.RegistryConfiguration.Url),
                configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
                configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            )
            .ClientId(
                configuration.GetValue(
                    "Krimson:Output:ClientId",
                    configuration.GetValue("Krimson:ClientId", Options.ProducerConfiguration.ClientId)
                )
            )
            .Topic(configuration.GetValue("Krimson:Output:Topic", ""));
    }


    public KrimsonProducer Create() {
        //TODO SS: replace ensure by more specific and granular validation
        Ensure.NotNullOrWhiteSpace(Options.ProducerConfiguration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.ProducerConfiguration.BootstrapServers, nameof(Options.ProducerConfiguration.BootstrapServers));
        Ensure.NotNullOrWhiteSpace(Options.RegistryConfiguration.Url, nameof(Options.RegistryConfiguration.Url));
        
        //var logger = Options.LoggerFactory.CreateLogger<KrimsonProducer>();
        
        var interceptors = Options.Interceptors
            .Prepend(new KrimsonProducerLogger())
            .Prepend(new ConfluentProducerLogger())
            .WithLoggerFactory(Options.LoggerFactory);

        return new(
            Options.ProducerConfiguration,
            interceptors.Intercept,
            Options.SerializerFactory(Options.RegistryFactory()),
            Options.DefaultTopic
        );
    }
}