using Confluent.Kafka;

namespace Krimson;

[PublicAPI]
public record ClientConnection {
    public string           BootstrapServers { get; init; } = "localhost:9092";
    public string           Username         { get; init; } = "";
    public string           Password         { get; init; } = "";
    public SecurityProtocol SecurityProtocol { get; init; } = SecurityProtocol.Plaintext;
    public SaslMechanism    SaslMechanism    { get; init; } = SaslMechanism.Plain;
}