using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using static System.String;

namespace Krimson.Readers.Configuration;

[PublicAPI]
public record KrimsonReaderBuilder {
    protected internal KrimsonReaderOptions Options { get; init; } = new();

    public KrimsonReaderBuilder Connection(
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
        );
    }

    public KrimsonReaderBuilder Connection(ClientConnection connection) {
        return Connection(
            connection.BootstrapServers, connection.Username, connection.Password,
            connection.SecurityProtocol, connection.SaslMechanism
        );
    }

    public KrimsonReaderBuilder ClientId(string clientId) {
        return OverrideConsumerConfig(
                cfg => {
                    cfg.ClientId = clientId;
                    cfg.GroupId  = IsNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId) ? clientId : cfg.GroupId;
                }
            );

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

    public KrimsonReaderBuilder GroupId(string groupId) {
        return OverrideConsumerConfig(cfg => {
            cfg.GroupId  = groupId;
            cfg.ClientId = Options.ConsumerConfiguration.ClientId == "rdkafka" ? groupId : cfg.ClientId;
        });
    }

    public KrimsonReaderBuilder OverrideConsumerConfig(Action<ConsumerConfig> configureConsumer) {
        Ensure.NotNull(configureConsumer, nameof(configureConsumer));

        return this with {
            Options = Options with {
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration).With(configureConsumer)
            }
        };
    }

    public KrimsonReaderBuilder OverrideSchemaRegistryConfig(Action<SchemaRegistryConfig> configureSchemaRegistry) {
        Ensure.NotNull(configureSchemaRegistry, nameof(configureSchemaRegistry));

        var options = Options with { };

        configureSchemaRegistry(options.RegistryConfiguration);

        return this with {
            Options = options
        };
    }

    // public KrimsonReaderBuilder SchemaRegistry(string url, string apiKey = "", string apiSecret = "") {
    //     return OverrideSchemaRegistryConfig(
    //         cfg => {
    //             cfg.Url                        = url;
    //             cfg.BasicAuthUserInfo          = $"{apiKey}:{apiSecret}";
    //             cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
    //         }
    //     );
    // }

    public KrimsonReaderBuilder SchemaRegistry(ISchemaRegistryClient schemaRegistryClient) {
        Ensure.NotNull(schemaRegistryClient, nameof(schemaRegistryClient));

        return this with {
            Options = Options with {
                RegistryFactory = () => schemaRegistryClient
            }
        };
    }

    // public KrimsonReaderBuilder SchemaRegistry(Func<ISchemaRegistryClient> factory) {
    //     Ensure.NotNull(factory, nameof(factory));
    //
    //     return this with {
    //         Options = Options with {
    //             RegistryFactory = factory
    //         }
    //     };
    // }

    public KrimsonReaderBuilder Intercept(InterceptorModule interceptor, bool prepend = false) {
        Ensure.NotNull(interceptor, nameof(interceptor));

        return this with {
            Options = Options with {
                Interceptors = prepend
                    ? Options.Interceptors.Prepend(interceptor)
                    : Options.Interceptors.Append(interceptor)
            }
        };
    }
    
    public KrimsonReaderBuilder Deserializer(Func<ISchemaRegistryClient, IDynamicDeserializer> getDeserializer) {
        return this with {
            Options = Options with {
                DeserializerFactory = Ensure.NotNull(getDeserializer, nameof(getDeserializer))
            }
        };
    }
    
    public KrimsonReaderBuilder EnableConsumerDebug(bool enable = true, string? context = null) {
        return OverrideConsumerConfig(cfg => cfg.EnableDebug(enable, context));
    }

    public KrimsonReaderBuilder ReadSettings(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", Options.ConsumerConfiguration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", Options.ConsumerConfiguration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", Options.ConsumerConfiguration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", Options.ConsumerConfiguration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", Options.ConsumerConfiguration.SaslMechanism!.Value)
            )
            // .SchemaRegistry(
            //     configuration.GetValue("Krimson:SchemaRegistry:Url", Options.RegistryConfiguration.Url),
            //     configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
            //     configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            // )
            .ClientId(
                configuration.GetValue(
                    "Krimson:Input:ClientId",
                    configuration.GetValue("Krimson:ClientId", Options.ConsumerConfiguration.ClientId)
                )
            )
            .GroupId(configuration.GetValue("Krimson:GroupId", Options.ConsumerConfiguration.GroupId));
    }

    public KrimsonReader Create() {
        //TODO SS: replace ensure by more specific and granular validation on processor builder
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId, nameof(GroupId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.BootstrapServers, nameof(Options.ConsumerConfiguration.BootstrapServers));
        Ensure.NotNullOrWhiteSpace(Options.RegistryConfiguration.Url, nameof(Options.RegistryConfiguration.Url));
        Ensure.NotNull(Options.DeserializerFactory, nameof(Deserializer));

        Ensure.Valid(Options.Router, nameof(Options.Router), router => router.HasRoutes);

        return new KrimsonReader(Options with { });
    }
}