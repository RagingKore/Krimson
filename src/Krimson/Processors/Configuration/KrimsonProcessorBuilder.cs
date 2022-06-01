using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using static System.String;

namespace Krimson.Processors.Configuration;

[PublicAPI]
public record KrimsonProcessorBuilder {
    protected internal KrimsonProcessorOptions Options { get; init; } = new();

    public KrimsonProcessorBuilder Connection(
        string bootstrapServers, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        return OverrideConsumerConfig(
                cfg => {
                    cfg.BootstrapServers = bootstrapServers;
                    cfg.SaslUsername     = username ?? "";
                    cfg.SaslPassword     = password ?? "";
                    cfg.SecurityProtocol = protocol;
                    cfg.SaslMechanism    = mechanism;
                }
            )
            .OverrideProducerConfig(
                cfg => {
                    cfg.BootstrapServers = bootstrapServers;
                    cfg.SaslUsername     = username ?? "";
                    cfg.SaslPassword     = password ?? "";
                    cfg.SecurityProtocol = protocol;
                    cfg.SaslMechanism    = mechanism;
                }
            );
    }

    public KrimsonProcessorBuilder Connection(ClientConnection connection) {
        return Connection(
            connection.BootstrapServers, connection.Username, connection.Password,
            connection.SecurityProtocol, connection.SaslMechanism
        );
    }

    public KrimsonProcessorBuilder ClientId(string clientId) {
        return OverrideConsumerConfig(
                cfg => {
                    cfg.ClientId = clientId;
                    cfg.GroupId  = IsNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId) ? clientId : cfg.GroupId;
                }
            )
            .OverrideProducerConfig(cfg => cfg.ClientId = clientId);

        // return this with {
        //     Options = Options with {
        //         ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration)
        //             .With(x => x.ClientId = clientId)
        //             .With(x => x.GroupId = clientId, Options.ConsumerConfiguration.GroupId is null),
        //         ProducerConfiguration = new ProducerConfig(Options.ProducerConfiguration)
        //             .With(x => x.ClientId = clientId)
        //     }
        // };
    }

    public KrimsonProcessorBuilder GroupId(string groupId) {
        return OverrideConsumerConfig(cfg => {
            cfg.GroupId  = groupId;
            cfg.ClientId = Options.ConsumerConfiguration.ClientId == "rdkafka" ? groupId : cfg.ClientId;
        });
    }

    public KrimsonProcessorBuilder InputTopic(params string[] topics) {
        return this with {
            Options = Options with {
                InputTopics = Options.InputTopics.Concat(topics).Distinct().ToArray()
            }
        };
    }

    public KrimsonProcessorBuilder OutputTopic(
        string topic, int partitions = 1, short replicationFactor = 3, Dictionary<string, string>? configuration = null
    ) {
        return this with {
            Options = Options with {
                OutputTopic = new() {
                    Name              = topic,
                    NumPartitions     = partitions,
                    ReplicationFactor = replicationFactor,
                    Configs           = configuration
                }
            }
        };
    }

    public KrimsonProcessorBuilder OverrideConsumerConfig(Action<ConsumerConfig> configureConsumer) {
        Ensure.NotNull(configureConsumer, nameof(configureConsumer));

        return this with {
            Options = Options with {
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration).With(configureConsumer)
            }
        };
    }

    public KrimsonProcessorBuilder OverrideProducerConfig(Action<ProducerConfig> configureProducer) {
        Ensure.NotNull(configureProducer, nameof(configureProducer));

        return this with {
            Options = Options with {
                ProducerConfiguration = new ProducerConfig(Options.ProducerConfiguration).With(configureProducer)
            }
        };
    }

    public KrimsonProcessorBuilder OverrideSchemaRegistryConfig(Action<SchemaRegistryConfig> configureSchemaRegistry) {
        Ensure.NotNull(configureSchemaRegistry, nameof(configureSchemaRegistry));

        var options = Options with { };

        configureSchemaRegistry(options.RegistryConfiguration);

        return this with {
            Options = options
        };
    }

    public KrimsonProcessorBuilder SchemaRegistry(string url, string apiKey, string apiSecret) {
        return OverrideSchemaRegistryConfig(
            cfg => {
                cfg.Url                        = url;
                cfg.BasicAuthUserInfo          = $"{apiKey}:{apiSecret}";
                cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
            }
        );
    }

    public KrimsonProcessorBuilder SchemaRegistry(ISchemaRegistryClient schemaRegistryClient) {
        Ensure.NotNull(schemaRegistryClient, nameof(schemaRegistryClient));

        return this with {
            Options = Options with {
                RegistryFactory = () => schemaRegistryClient
            }
        };
    }

    public KrimsonProcessorBuilder SchemaRegistry(Func<ISchemaRegistryClient> factory) {
        Ensure.NotNull(factory, nameof(factory));

        return this with {
            Options = Options with {
                RegistryFactory = factory
            }
        };
    }

    public KrimsonProcessorBuilder Intercept(InterceptorModule interceptor, bool prepend = false) {
        Ensure.NotNull(interceptor, nameof(interceptor));

        return this with {
            Options = Options with {
                Interceptors = prepend
                    ? Options.Interceptors.Prepend(interceptor)
                    : Options.Interceptors.Append(interceptor)
            }
        };
    }

    public KrimsonProcessorBuilder Serializer(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        return this with {
            Options = Options with {
                SerializerFactory = Ensure.NotNull(getSerializer, nameof(getSerializer))
            }
        };
    }

    public KrimsonProcessorBuilder Deserializer(Func<ISchemaRegistryClient, IDynamicDeserializer> getDeserializer) {
        return this with {
            Options = Options with {
                DeserializerFactory = Ensure.NotNull(getDeserializer, nameof(getDeserializer))
            }
        };
    }

    public KrimsonProcessorBuilder Module(KrimsonProcessorModule module) {
        return this with {
            Options = Options with {
                Router = Ensure.NotNull(module, nameof(module)).Router
            }
        };
    }

    public KrimsonProcessorBuilder Module<T>() where T : KrimsonProcessorModule, new() {
        return this with {
            Options = Options with {
                Router = new T().Router
            }
        };
    }

    public KrimsonProcessorBuilder Process<T>(ProcessMessageAsync<T> handler) {
        return this with {
            Options = Options with {
                Router = Options.Router.Register(Ensure.NotNull(handler, nameof(handler)))
            }
        };
    }

    public KrimsonProcessorBuilder Process<T>(ProcessMessage<T> handler) {
        return this with {
            Options = Options with {
                Router = Options.Router.Register(Ensure.NotNull(handler, nameof(handler)))
            }
        };
    }

    public KrimsonProcessorBuilder LoggerFactory(ILoggerFactory loggerFactory) {
        return this with {
            Options = Options with {
                LoggerFactory = Ensure.NotNull(loggerFactory, nameof(loggerFactory))
            }
        };
    }

    public KrimsonProcessorBuilder EnableConsumerDebug(bool enable = true, string? context = null) {
        return OverrideConsumerConfig(cfg => cfg.EnableDebug(enable, context));
    }

    public KrimsonProcessorBuilder EnableProducerDebug(bool enable = true, string? context = null) {
        return OverrideProducerConfig(cfg => cfg.EnableDebug(enable, context));
    }
    
    public KrimsonProcessorBuilder ReadSettings(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        //TODO SS: create awesome connection string convention for kafka and registry
        //kafka://username:password@localhost:9092?securityProtocol=SaslSsl&saslMechanism=Plain
        //schema://username:password@localhost:8081?requestTimeoutMs=30000
        // configuration.GetValue("Krimson:ConnectionString", "kafka://krimson:krimson@localhost:9092?securityProtocol=Plaintext&saslMechanism=Plain");
        // configuration.GetValue("Krimson:SchemaRegistry:ConnectionString", "schema://krimson:krimson@localhost:8081?requestTimeoutMs=30000");
        // all indexes are wrong lol. this was just a brain dump
        // static ClientConnection ParseKafkaConnection(string connectionString) {
        //
        //     var value = connectionString;
        //     
        //     if (connectionString.StartsWith("kafka://")) {
        //         value = connectionString[..7];
        //     }
        //     
        //     var credentialsMarkerIndex = value.IndexOf('@');
        //     var credentials            = value[..credentialsMarkerIndex].Split(':');
        //     var username               = credentials[0];
        //     var password               = credentials[1];
        //     
        //     var urlMarkerIndex = value.IndexOf('?');
        //
        //     if (urlMarkerIndex == -1) {
        //         var url = value[credentialsMarkerIndex..];
        //
        //         return new() {
        //             BootstrapServers = url,
        //             Username         = username,
        //             Password         = password
        //         };
        //     }
        //     else {
        //         var url     = value[credentialsMarkerIndex..urlMarkerIndex];
        //         var options = value[urlMarkerIndex..].Split('&');
        //
        //         // initially yes, just make it a fixed thing
        //         Enum.TryParse(options[0], true, out SecurityProtocol securityProtocol);
        //         Enum.TryParse(options[1], true, out SaslMechanism saslMechanism);
        //
        //         return new() {
        //             BootstrapServers = url,
        //             Username         = username,
        //             Password         = password,
        //             SecurityProtocol = securityProtocol,
        //             SaslMechanism    = saslMechanism
        //         };
        //     }
        // }

        return Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", Options.ConsumerConfiguration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", Options.ConsumerConfiguration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", Options.ConsumerConfiguration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", Options.ConsumerConfiguration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", Options.ConsumerConfiguration.SaslMechanism!.Value)
            )
            .SchemaRegistry(
                configuration.GetValue("Krimson:SchemaRegistry:Url", Options.RegistryConfiguration.Url),
                configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
                configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            )
            .ClientId(
                configuration.GetValue(
                    "Krimson:Input:ClientId",
                    configuration.GetValue("Krimson:ClientId", Options.ConsumerConfiguration.ClientId)
                )
            )
            .GroupId(configuration.GetValue("Krimson:GroupId", Options.ConsumerConfiguration.GroupId))
            .InputTopic(configuration.GetValues("Krimson:Input:Topic"))
            .OutputTopic(configuration.GetValue("Krimson:Output:Topic", ""));
    }

    public KrimsonProcessor Create() {
        //TODO SS: replace ensure by more specific and granular validation on processor builder
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId, nameof(GroupId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.BootstrapServers, nameof(Options.ConsumerConfiguration.BootstrapServers));
        Ensure.NotNullOrWhiteSpace(Options.ProducerConfiguration.BootstrapServers, nameof(Options.ProducerConfiguration.BootstrapServers));
        Ensure.NotNullOrWhiteSpace(Options.RegistryConfiguration.Url, nameof(Options.RegistryConfiguration.Url));
        Ensure.NotNullOrEmpty(Options.InputTopics, nameof(InputTopic));
        Ensure.Valid(Options.Router, nameof(Options.Router), router => router.HasRoutes);

        return new(Options with { });
    }

    // not yet
    //
    // public IEnumerable<KrimsonProcessor> Create() {
    //     for (var i = 1; i <= Options.Tasks; i++) {
    //         var builder = this with { };
    //
    //         yield return builder
    //             .SubscriptionName(builder.Options.ConsumerConfiguration.GroupId)
    //             .ProcessorName($"{builder.Options.ConsumerConfiguration.ClientId}-{i:000}")
    //             .Create();
    //     }
    // }
}