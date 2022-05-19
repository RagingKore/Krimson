using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Microsoft.Extensions.Configuration;
using static Krimson.DefaultConfigs;

namespace Krimson.Processors.Configuration;

[PublicAPI]
public record KrimsonProcessorBuilder {
    protected internal KrimsonProcessorOptions Options { get; init; } = new();

    public KrimsonProcessorBuilder Connection(
        string bootstrapServers, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        Ensure.NotNullOrWhiteSpace(bootstrapServers, nameof(bootstrapServers));

        return new() {
            Options = Options with {
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration)
                    .With(
                        cfg => {
                            cfg.BootstrapServers = bootstrapServers;
                            cfg.SaslUsername     = username ?? "";
                            cfg.SaslPassword     = password ?? "";
                            cfg.SecurityProtocol = protocol;
                            cfg.SaslMechanism    = mechanism;
                        }
                    ),
                ProducerConfiguration = new ProducerConfig(Options.ProducerConfiguration)
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

    public KrimsonProcessorBuilder Connection(ClientConnection connection) =>
        Connection(
            connection.BootstrapServers, connection.Username, connection.Password,
            connection.SecurityProtocol, connection.SaslMechanism
        );

    public KrimsonProcessorBuilder ProcessorName(string processorName) {
        Ensure.NotNullOrWhiteSpace(processorName, nameof(processorName));

        return this with {
            Options = Options with {
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration)
                    .With(x => x.ClientId = processorName)
                    .With(x => x.GroupId = processorName, Options.ConsumerConfiguration.GroupId is null),
                ProducerConfiguration = new ProducerConfig(Options.ProducerConfiguration)
                    .With(x => x.ClientId = processorName)
            }
        };
    }

    public KrimsonProcessorBuilder SubscriptionName(string subscriptionName) {
        Ensure.NotNullOrWhiteSpace(subscriptionName, nameof(subscriptionName));

        return this with {
            Options = Options with {
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration)
                    .With(x => x.GroupId = subscriptionName)
                    .With(x => x.ClientId = subscriptionName, Options.ConsumerConfiguration.ClientId == "rdkafka")
            }
        };
    }

    public KrimsonProcessorBuilder InputTopics(params string[] topics) {
        Ensure.NotNullOrEmpty(topics, nameof(topics))
            .ForEach(topic => Ensure.NotNullOrWhiteSpace(topic, nameof(topic)));

        return this with {
            Options = Options with {
                InputTopics = Options.InputTopics.Concat(topics).ToArray()
            }
        };
    }

    public KrimsonProcessorBuilder InputTopic(string topic) => InputTopics(topic);

    public KrimsonProcessorBuilder OutputTopic(
        string topic, int partitions = 1, short replicationFactor = 3, Dictionary<string, string>? configuration = null
    ) {
        Ensure.NotNullOrWhiteSpace(topic, nameof(topic));

        return this with {
            Options = Options with {
                OutputTopic = new TopicSpecification {
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
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration)
                    .With(configureConsumer)
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

    public KrimsonProcessorBuilder Intercept(InterceptorModule interceptor) {
        return this with {
            Options = Options with {
                Interceptors = Options.Interceptors.With(x => x.Add(Ensure.NotNull(interceptor, nameof(interceptor))))
            }
        };
    }

    public KrimsonProcessorBuilder SchemaRegistry(Action<SchemaRegistryConfig> configure) {
        Ensure.NotNull(configure, nameof(configure));

        return this with {
            Options = Options with {
                RegistryFactory = () => new CachedSchemaRegistryClient(DefaultSchemaRegistryConfig.With(configure))
            }
        };
    }

    public KrimsonProcessorBuilder SchemaRegistry(string url, string apiKey, string apiSecret) {
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

    public KrimsonProcessorBuilder Configure(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return ProcessorName(configuration.GetValue("Krimson:Processor:Name", Options.ConsumerConfiguration.ClientId))
            .SubscriptionName(configuration.GetValue("Krimson:Processor:Subscription", Options.ConsumerConfiguration.GroupId))
            .Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", Options.ConsumerConfiguration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", Options.ConsumerConfiguration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", Options.ConsumerConfiguration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", Options.ConsumerConfiguration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", Options.ConsumerConfiguration.SaslMechanism!.Value)
            )
            .SchemaRegistry(
                configuration.GetValue("Krimson:SchemaRegistry:Url", DefaultSchemaRegistryConfig.Url),
                configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
                configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            );
    }

    // public KrimsonProcessorBuilder Tasks(int tasks) {
    //     Ensure.Positive(tasks, nameof(tasks));
    //
    //     return this with {
    //         Options = Options with {
    //             Tasks = tasks
    //         }
    //     };
    // }

    public KrimsonProcessor Create() {
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.ClientId, nameof(ProcessorName));
        Ensure.NotNullOrEmpty(Options.InputTopics, nameof(Options.InputTopics));
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
 
   