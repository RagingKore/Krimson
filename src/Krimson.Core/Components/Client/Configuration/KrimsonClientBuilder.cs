using Confluent.Kafka;
using Krimson.Interceptors;
using Microsoft.Extensions.Configuration;
using static System.String;

namespace Krimson.Client.Configuration;

[PublicAPI]
public record KrimsonClientBuilder {
    protected internal KrimsonClientOptions Options { get; init; } = new();

    public KrimsonClientBuilder OverrideConfiguration(Action<ClientConfig> configure) {
        Ensure.NotNull(configure, nameof(configure));

        return this with {
            Options = Options with {
                Configuration = new AdminClientConfig(Options.Configuration).With(configure)
            }
        };
    }

    public KrimsonClientBuilder Connection(
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

    public KrimsonClientBuilder Connection(ClientConnection connection) {
        return Connection(
            connection.BootstrapServers, connection.Username, connection.Password,
            connection.SecurityProtocol, connection.SaslMechanism
        );
    }

    public KrimsonClientBuilder ClientId(string clientId) {
        return IsNullOrWhiteSpace(clientId) ? this : OverrideConfiguration(cfg => cfg.ClientId = clientId);
    }

    public KrimsonClientBuilder Intercept(InterceptorModule interceptor, bool prepend = false) {
        Ensure.NotNull(interceptor, nameof(interceptor));

        return this with {
            Options = Options with {
                Interceptors = prepend
                    ? Options.Interceptors.Prepend(interceptor)
                    : Options.Interceptors.Append(interceptor)
            }
        };
    }

    public KrimsonClientBuilder ReadSettings(IConfiguration configuration) {
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
                    "Krimson:Input:ClientId",
                    configuration.Value(
                        "Krimson:ClientId", 
                        configuration.Value(
                            "ASPNETCORE_APPLICATIONNAME", 
                            Options.Configuration.ClientId
                        )
                    )
                )!
            );
    }

    public KrimsonClient Create() {
        //TODO SS: replace ensure by more specific and granular validation on processor builder
        Ensure.NotNullOrWhiteSpace(Options.Configuration.ClientId, nameof(ClientId));
        Ensure.NotNullOrWhiteSpace(Options.Configuration.BootstrapServers, nameof(Options.Configuration.BootstrapServers));

        return new(Options with { });
    }
}