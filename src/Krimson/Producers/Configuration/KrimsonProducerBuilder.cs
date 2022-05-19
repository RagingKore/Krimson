using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.SchemaRegistry.Protobuf;
using Microsoft.Extensions.Configuration;

namespace Krimson.Producers;

[PublicAPI]
public record KrimsonProducerBuilder {
    public KrimsonProducerOptions Options { get; init; } = new();

    public KrimsonProducerBuilder Configure(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", Options.Configuration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", Options.Configuration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", Options.Configuration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", Options.Configuration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", Options.Configuration.SaslMechanism!.Value)
            )
            .SchemaRegistry(
                configuration.GetValue("Krimson:SchemaRegistry:Url", DefaultConfigs.DefaultSchemaRegistryConfig.Url),
                configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
                configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            );
    }
    
    // public KafkaProducerBuilder Configure(IConfiguration configuration) {
    //     Ensure.NotNull(configuration, nameof(configuration));
    //
    //     var connection = new ClientConnection {
    //         Url = configuration.GetValue(
    //             "Kafka:Connection:BootstrapServers",
    //             configuration.GetValue(
    //                 "Kafka:Connection:ServiceUrl",
    //                 configuration.GetValue(
    //                     "Kafka:Connection:Url",
    //                     configuration.GetValue(
    //                         "Kafka:Connection:Link",
    //                         DefaultConfigs.DefaultClientConfig.BootstrapServers
    //                     )
    //                 )
    //             )
    //         ),
    //         Username = configuration.GetValue("Kafka:Connection:Username", string.Empty),
    //         Password = configuration.GetValue("Kafka:Connection:Password", string.Empty),
    //         UseSSl   = configuration.GetValue("Kafka:Connection:UseSsl", true)
    //     };
    //
    //     var compression = new CompressionOptions {
    //         Enabled = configuration.GetValue("Kafka:Compression:Enabled", true),
    //         Type    = configuration.GetValue("Kafka:Compression:Type", Krimson.Configuration.CompressionType.Zstd),
    //         Level   = configuration.GetValue("Kafka:Compression:Level", CompressionOptions.ZstdDefaultLevel)
    //     };
    //
    //     if (compression.Type == Krimson.Configuration.CompressionType.Zstd) {
    //         if (compression.Level > CompressionOptions.ZstdMaxLevel)
    //             compression = compression with {
    //                 Level = CompressionOptions.ZstdMaxLevel
    //             };
    //
    //         if (compression.Level < CompressionOptions.ZstdMinLevel)
    //             compression = compression with {
    //                 Level = CompressionOptions.ZstdDefaultLevel
    //             };
    //     }
    //
    //     var schemaRegistryConfig = DefaultConfigs.DefaultSchemaRegistryConfig
    //         .With(cfg => cfg.Url = configuration.GetValue("Kafka:SchemaRegistry:Url", DefaultConfigs.DefaultSchemaRegistryConfig.Url))
    //         .With(cfg => cfg.BasicAuthUserInfo = configuration.GetValue("Kafka:SchemaRegistry:ApiKey", string.Empty))
    //         .With(cfg => cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo);
    //
    //     return this with {
    //         Options = Options with {
    //             Connection  = connection,
    //             Compression = compression,
    //             GetRegistry = () => new CachedSchemaRegistryClient(schemaRegistryConfig)
    //         }
    //     };
    // }

    // public KafkaProducerBuilder Connection(string brokerUrl, string? username = null, string? password = null, bool ssl = false) =>
    //     new() {
    //         Options = Options with {
    //             Connection = new ClientConnection {
    //                 BootstrapServers      = Ensure.NotNullOrWhiteSpace(brokerUrl, nameof(brokerUrl)),
    //                 Username = username ?? string.Empty,
    //                 Password = password ?? string.Empty,
    //                 UseSSl   = ssl
    //             }
    //         }
    //     };

   public KrimsonProducerBuilder Connection(
        string bootstrapServers, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        Ensure.NotNullOrWhiteSpace(bootstrapServers, nameof(bootstrapServers));
            
        return new() {
            Options = Options with {
                Configuration = new ProducerConfig(Options.Configuration)
                    .With(
                        cfg => {
                            cfg.BootstrapServers = bootstrapServers;
                            cfg.SaslUsername     = username ?? "";
                            cfg.SaslPassword     = password ?? "";
                            cfg.SecurityProtocol = protocol;
                            cfg.SaslMechanism    = mechanism;
                        }
                    )
            }
        };
    }

   public KrimsonProducerBuilder Connection(ClientConnection connection) =>
       Connection(
           connection.BootstrapServers, connection.Username, connection.Password,
           connection.SecurityProtocol, connection.SaslMechanism
       );

    public KrimsonProducerBuilder ProducerName(string producerName) {
        Ensure.NotNullOrWhiteSpace(producerName, nameof(producerName));
            
        return this with {
            Options = Options with {
                Configuration = new ProducerConfig(Options.Configuration)
                    .With(x => x.ClientId = producerName)
            }
        };
    }

    public KrimsonProducerBuilder SchemaRegistry(Action<SchemaRegistryConfig> configure) {
        Ensure.NotNull(configure, nameof(configure));

        return this with {
            Options = Options with {
                RegistryFactory = () => new CachedSchemaRegistryClient(DefaultConfigs.DefaultSchemaRegistryConfig.With(configure))
            }
        };
    }

    public KrimsonProducerBuilder SchemaRegistry(string url, string apiKey, string apiSecret) {
        Ensure.NotNullOrWhiteSpace(url, nameof(url));
        Ensure.NotNullOrWhiteSpace(apiKey, nameof(apiKey));
        Ensure.NotNullOrWhiteSpace(apiSecret, nameof(apiSecret));

        return SchemaRegistry(
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
    
    // public KafkaProducerBuilder SchemaRegistry(Action<SchemaRegistryConfig> configure) {
    //     Ensure.NotNull(configure, nameof(configure));
    //     
    //     return this with {
    //         Options = Options with {
    //             GetRegistry = () => new CachedSchemaRegistryClient(DefaultConfigs.DefaultSchemaRegistryConfig.With(configure))
    //         }
    //     };
    // }
    //
    // // public KafkaProducerBuilder SchemaRegistry(string url, string apiKey) {
    // //     return SchemaRegistry(cfg => {
    // //             cfg.Url                        = Ensure.NotNullOrWhiteSpace(url, nameof(url));
    // //             cfg.BasicAuthUserInfo          = Ensure.NotNullOrWhiteSpace(apiKey, nameof(apiKey));
    // //             cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
    // //         }
    // //     );
    // // }
    //
    // public KafkaProducerBuilder SchemaRegistry(Func<ISchemaRegistryClient> registryFactory) {
    //     Ensure.NotNull(registryFactory, nameof(registryFactory));
    //
    //     return this with {
    //         Options = Options with {
    //             GetRegistry = registryFactory
    //         }
    //     };
    // }
    //
    // public KafkaProducerBuilder SchemaRegistry(ISchemaRegistryClient schemaRegistryClient) {
    //     Ensure.NotNull(schemaRegistryClient, nameof(schemaRegistryClient));
    //
    //     return this with {
    //         Options = Options with {
    //             GetRegistry = () => schemaRegistryClient
    //         }
    //     };
    // }
    
    public KrimsonProducerBuilder Serializer(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        return this with {
            Options = Options with {
                SerializerFactory = Ensure.NotNull(getSerializer, nameof(getSerializer))
            }
        };
    }

    public KrimsonProducerBuilder UseProtobuf(Action<ProtobufSerializerConfig>? configure) =>
        Serializer(registry => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configure?.Invoke(x))));

    public KrimsonProducerBuilder Topic(string? topic) {
        if (topic != null) // we accept nulls
            Ensure.NotNullOrWhiteSpace(topic, nameof(topic));

        return this with {
            Options = Options with {
                DefaultTopic = topic
            }
        };
    }

    public KrimsonProducerBuilder Configuration(Action<ProducerConfig> configure) {
        Ensure.NotNull(configure, nameof(configure));
        
        return this with {
            Options = Options with {
                Configuration = Options.Configuration.With(configure)
            }
        };
    }

    public KrimsonProducerBuilder Configuration(ProducerConfig configuration) =>
        this with {
            Options = Options with {
                Configuration = Ensure.NotNull(configuration, nameof(configuration))
            }
        };

    public KrimsonProducerBuilder Intercept(InterceptorModule interceptor) =>
        this with {
            Options = Options with {
                Interceptors = Options.Interceptors.With(x => x.Add(Ensure.NotNull(interceptor, nameof(interceptor))))
            }
        };

    public KrimsonProducer Create() {
        Ensure.NotNullOrWhiteSpace(Options.Configuration.ClientId, nameof(ProducerName));
        Ensure.NotNullOrWhiteSpace(Options.Configuration.BootstrapServers, nameof(Options.Configuration.BootstrapServers));
        
        var options = Options with { };

        return new KrimsonProducer(
            options.Configuration,
            options.Interceptors.Intercept,
            options.SerializerFactory(options.RegistryFactory()),
            options.DefaultTopic
        );
    }
}