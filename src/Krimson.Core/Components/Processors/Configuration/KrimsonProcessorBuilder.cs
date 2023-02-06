using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Persistence.State;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using static System.String;

namespace Krimson.Processors.Configuration;

[PublicAPI]
public record KrimsonProcessorBuilder {
    protected internal KrimsonProcessorOptions Options { get; init; } = new();

    public KrimsonProcessorBuilder OverrideConsumerConfiguration(Action<ConsumerConfig> configureConsumer) {
        Ensure.NotNull(configureConsumer, nameof(configureConsumer));

        return this with {
            Options = Options with {
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration).With(configureConsumer)
            }
        };
    }

    public KrimsonProcessorBuilder DisableAutoCommit() =>
        OverrideConsumerConfiguration(x => x.EnableAutoCommit = false);

    public KrimsonProcessorBuilder OverrideProducerConfiguration(Action<ProducerConfig> configureProducer) {
        Ensure.NotNull(configureProducer, nameof(configureProducer));

        return this with {
            Options = Options with {
                ProducerConfiguration = new ProducerConfig(Options.ProducerConfiguration).With(configureProducer)
            }
        };
    }

    public KrimsonProcessorBuilder Connection(
        string? bootstrapServers = null, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        return OverrideConsumerConfiguration(
                cfg => {
                    cfg.BootstrapServers = bootstrapServers ?? Options.ConsumerConfiguration.BootstrapServers;
                    cfg.SaslUsername     = username         ?? Options.ConsumerConfiguration.SaslUsername;
                    cfg.SaslPassword     = password         ?? Options.ConsumerConfiguration.SaslPassword;
                    cfg.SecurityProtocol = protocol;
                    cfg.SaslMechanism    = mechanism;
                }
            )
            .OverrideProducerConfiguration(
                cfg => {
                    cfg.BootstrapServers = bootstrapServers ?? Options.ConsumerConfiguration.BootstrapServers;
                    cfg.SaslUsername     = username         ?? Options.ConsumerConfiguration.SaslUsername;
                    cfg.SaslPassword     = password         ?? Options.ConsumerConfiguration.SaslPassword;
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
        return IsNullOrWhiteSpace(clientId) 
            ? this 
            : OverrideConsumerConfiguration(
                cfg => {
                    cfg.ClientId = clientId;
                    cfg.GroupId  = IsNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId) ? clientId : cfg.GroupId;
                }
            )
            .OverrideProducerConfiguration(cfg => cfg.ClientId = clientId);
    }

    public KrimsonProcessorBuilder GroupId(string groupId) {
        return IsNullOrWhiteSpace(groupId) 
            ? this 
            : OverrideConsumerConfiguration(cfg => {
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

    public KrimsonProcessorBuilder OutputTopic(string topic, int partitions = 1, short replicationFactor = 3, Dictionary<string, string>? configuration = null) {
        return IsNullOrWhiteSpace(topic) ? this : this with {
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

    public KrimsonProcessorBuilder Serializer(Func<IDynamicSerializer> getSerializer) {
        return this with {
            Options = Options with {
                SerializerFactory = Ensure.NotNull(getSerializer, nameof(getSerializer))
            }
        };
    }

    public KrimsonProcessorBuilder Deserializer(Func<IDynamicDeserializer> getDeserializer) {
        return this with {
            Options = Options with {
                DeserializerFactory = Ensure.NotNull(getDeserializer, nameof(getDeserializer))
            }
        };
    }

    public KrimsonProcessorBuilder Module(Func<KrimsonProcessorModule> getModule) {
        Ensure.NotNull(getModule, nameof(getModule));
        return this with {
            Options = Options with {
                ModuleFactories = Options.ModuleFactories.With(x => x.Add(getModule))
            }
        };
    }

    public KrimsonProcessorBuilder Module(KrimsonProcessorModule module) {
        return this with {
            Options = Options with {
                ModuleFactories = Options.ModuleFactories.With(x => x.Add(() => module))
            }
        };
    }

    public KrimsonProcessorBuilder Modules(IEnumerable<KrimsonProcessorModule> modules) {
        return this with {
            Options = Options with {
                ModuleFactories = Options.ModuleFactories
                    .With(x => x.AddRange(modules.Select(module => { return new Func<KrimsonProcessorModule>(() => module); })))
            }
        };
    }

    public KrimsonProcessorBuilder Process<T>(ProcessMessageAsync<T> handler) {
        Ensure.NotNull(handler, nameof(handler));
        return Module(handler.AsModule());
    }

    public KrimsonProcessorBuilder Process<T>(ProcessMessage<T> handler) {
        Ensure.NotNull(handler, nameof(handler));
        return Module(handler.AsModule());
    }

    public KrimsonProcessorBuilder EnableConsumerDebug(bool enable = true, string? context = null) {
        return OverrideConsumerConfiguration(cfg => cfg.EnableDebug(enable, context));
    }

    public KrimsonProcessorBuilder EnableProducerDebug(bool enable = true, string? context = null) {
        return OverrideProducerConfiguration(cfg => cfg.EnableDebug(enable, context));
    }
    
    public KrimsonProcessorBuilder EnableDebug(bool enable = true) {
        return EnableConsumerDebug(enable).EnableProducerDebug(enable);
    }
    
    public KrimsonProcessorBuilder StateStore(Func<IStateStore> getStateStore) {
        return this with {
            Options = Options with {
                StateStoreFactory = Ensure.NotNull(getStateStore, nameof(getStateStore))
            }
        };
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
                configuration.Value("Krimson:Connection:BootstrapServers", Options.ConsumerConfiguration.BootstrapServers),
                configuration.Value("Krimson:Connection:Username", Options.ConsumerConfiguration.SaslUsername),
                configuration.Value("Krimson:Connection:Password", Options.ConsumerConfiguration.SaslPassword),
                configuration.Value("Krimson:Connection:SecurityProtocol", Options.ConsumerConfiguration.SecurityProtocol!.Value),
                configuration.Value("Krimson:Connection:SaslMechanism", Options.ConsumerConfiguration.SaslMechanism!.Value)
            )
            .ClientId(
                configuration.Value(
                    "Krimson:Input:ClientId",
                    configuration.Value(
                        "Krimson:ClientId", 
                        configuration.Value(
                            "ASPNETCORE_APPLICATIONNAME", 
                            Options.ConsumerConfiguration.ClientId
                        )
                    )
                )!
            )
            .GroupId(configuration.Value("Krimson:GroupId", Options.ConsumerConfiguration.GroupId))
            .InputTopic(configuration.Values("Krimson:Input:Topic"))
            .OutputTopic(configuration.Value("Krimson:Output:Topic", ""));
    }

    public KrimsonProcessor Create() {
        //TODO SS: replace ensure by more specific and granular validation on processor builder
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId, nameof(GroupId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.BootstrapServers, nameof(Options.ConsumerConfiguration.BootstrapServers));
        Ensure.NotNullOrWhiteSpace(Options.ProducerConfiguration.BootstrapServers, nameof(Options.ProducerConfiguration.BootstrapServers));
        Ensure.NotNullOrEmpty(Options.InputTopics, nameof(InputTopic));
        Ensure.NotNull(Options.SerializerFactory, nameof(Serializer));
        Ensure.NotNull(Options.DeserializerFactory, nameof(Deserializer));
        Ensure.Valid(Options.ModuleFactories, nameof(Modules), x => x.Any());

        return new(Options with { });
    }
}