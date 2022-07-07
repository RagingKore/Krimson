using Confluent.Kafka;

namespace Krimson;

[PublicAPI]
public record ClientConnection {
    public ClientConnection() { }

    public ClientConnection(string bootstrapServers, string username, string password, SecurityProtocol securityProtocol, SaslMechanism saslMechanism) {
        BootstrapServers = bootstrapServers;
        Username         = username;
        Password         = password;
        SecurityProtocol = securityProtocol;
        SaslMechanism    = saslMechanism;
    }

    public string           BootstrapServers { get; init; } = DefaultConfigs.DefaultClientConfig.BootstrapServers;
    public string           Username         { get; init; } = "";
    public string           Password         { get; init; } = "";
    public SecurityProtocol SecurityProtocol { get; init; } = DefaultConfigs.DefaultClientConfig.SecurityProtocol!.Value;
    public SaslMechanism    SaslMechanism    { get; init; } = DefaultConfigs.DefaultClientConfig.SaslMechanism!.Value;
}