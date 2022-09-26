using Confluent.Kafka;
using static System.String;
using static Krimson.DefaultConfigs;

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

    public string           BootstrapServers { get; init; } = DefaultClientConfig.BootstrapServers;
    public string           Username         { get; init; } = Empty;
    public string           Password         { get; init; } = Empty;
    public SecurityProtocol SecurityProtocol { get; init; } = DefaultClientConfig.SecurityProtocol!.Value;
    public SaslMechanism    SaslMechanism    { get; init; } = DefaultClientConfig.SaslMechanism!.Value;
}