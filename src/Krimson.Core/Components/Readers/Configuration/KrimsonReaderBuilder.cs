using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using static System.String;

namespace Krimson.Readers.Configuration;

[PublicAPI]
public record KrimsonReaderBuilder {
    protected internal KrimsonReaderOptions Options { get; init; } = new();

    public KrimsonReaderBuilder Connection(
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
        );
    }

    public KrimsonReaderBuilder Connection(ClientConnection connection) {
        return Connection(
            connection.BootstrapServers, connection.Username, connection.Password,
            connection.SecurityProtocol, connection.SaslMechanism
        );
    }

    public KrimsonReaderBuilder ClientId(string clientId) {
        return IsNullOrWhiteSpace(clientId) 
            ? this 
            : OverrideConsumerConfiguration(
                cfg => {
                    cfg.ClientId = clientId;
                    cfg.GroupId  = IsNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId) ? clientId : cfg.GroupId;
                }
            );
    }

    public KrimsonReaderBuilder GroupId(string groupId) {
        return IsNullOrWhiteSpace(groupId) 
            ? this 
            : OverrideConsumerConfiguration(cfg => {
                cfg.GroupId  = groupId;
                cfg.ClientId = Options.ConsumerConfiguration.ClientId == "rdkafka" ? groupId : cfg.ClientId;
            });
    }

    public KrimsonReaderBuilder OverrideConsumerConfiguration(Action<ConsumerConfig> configureConsumer) {
        Ensure.NotNull(configureConsumer, nameof(configureConsumer));

        return this with {
            Options = Options with {
                ConsumerConfiguration = new ConsumerConfig(Options.ConsumerConfiguration).With(configureConsumer)
            }
        };
    }
    
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
    
    public KrimsonReaderBuilder Deserializer(Func<IDynamicDeserializer> getDeserializer) {
        return this with {
            Options = Options with {
                DeserializerFactory = Ensure.NotNull(getDeserializer, nameof(getDeserializer))
            }
        };
    }
    
    public KrimsonReaderBuilder EnableConsumerDebug(bool enable = true, string? context = null) {
        return OverrideConsumerConfiguration(cfg => cfg.EnableDebug(enable, context));
    }

     public KrimsonReaderBuilder ReadSettings(IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));
        
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
            );
    }

    public KrimsonReader Create() {
        //TODO SS: replace ensure by more specific and granular validation on processor builder
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId, nameof(GroupId));
        Ensure.NotNullOrWhiteSpace(Options.ConsumerConfiguration.BootstrapServers, nameof(Options.ConsumerConfiguration.BootstrapServers));
        Ensure.NotNull(Options.DeserializerFactory, nameof(Deserializer));

        return new(Options with { });
    }
}