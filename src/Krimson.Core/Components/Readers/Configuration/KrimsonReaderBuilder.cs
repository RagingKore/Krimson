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
        string bootstrapServers, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        return OverrideConsumerConfiguration(
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
        return OverrideConsumerConfiguration(
                cfg => {
                    cfg.ClientId = clientId;
                    cfg.GroupId  = IsNullOrWhiteSpace(Options.ConsumerConfiguration.GroupId) ? clientId : cfg.GroupId;
                }
            );
    }

    public KrimsonReaderBuilder GroupId(string groupId) {
        return OverrideConsumerConfiguration(cfg => {
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
                configuration.GetValue("Krimson:Connection:BootstrapServers", Options.ConsumerConfiguration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", Options.ConsumerConfiguration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", Options.ConsumerConfiguration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", Options.ConsumerConfiguration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", Options.ConsumerConfiguration.SaslMechanism!.Value)
            )
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
        Ensure.NotNull(Options.DeserializerFactory, nameof(Deserializer));

        return new KrimsonReader(Options with { });
    }
}