using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using static System.String;

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
        string? bootstrapServers = null, string? username = null, string? password = null,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        return OverrideConfiguration(
            cfg => {
                cfg.BootstrapServers = bootstrapServers ?? Options.Configuration.BootstrapServers;
                cfg.SaslUsername     = username         ?? Options.Configuration.SaslUsername;
                cfg.SaslPassword     = password         ?? Options.Configuration.SaslPassword;
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
        return IsNullOrWhiteSpace(clientId) 
            ? this 
            : OverrideConfiguration(cfg => cfg.ClientId = clientId);
    }

    public KrimsonProducerBuilder Serializer(Func<IDynamicSerializer> getSerializer) {
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
                configuration.Value("Krimson:Connection:BootstrapServers", Options.Configuration.BootstrapServers),
                configuration.Value("Krimson:Connection:Username", Options.Configuration.SaslUsername),
                configuration.Value("Krimson:Connection:Password", Options.Configuration.SaslPassword),
                configuration.Value("Krimson:Connection:SecurityProtocol", Options.Configuration.SecurityProtocol!.Value),
                configuration.Value("Krimson:Connection:SaslMechanism", Options.Configuration.SaslMechanism!.Value)
            )
            .ClientId(
                configuration.Value(
                    "Krimson:Output:ClientId",
                    configuration.Value(
                        "Krimson:ClientId", 
                        configuration.Value(
                            "ASPNETCORE_APPLICATIONNAME", 
                            Options.Configuration.ClientId
                        )
                    )
                )
            )
            .Topic(configuration.GetValue("Krimson:Output:Topic", ""));
    }
    
    public KrimsonProducer Create() {
        //TODO SS: replace ensure by more specific and granular validation
        Ensure.NotNullOrWhiteSpace(Options.Configuration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.Configuration.BootstrapServers, nameof(Options.Configuration.BootstrapServers));
        Ensure.NotNull(Options.SerializerFactory, nameof(Serializer));

        return new(Options with { });
    }
}