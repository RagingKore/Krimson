using Confluent.Kafka;
using Confluent.SchemaRegistry;
using static Confluent.Kafka.AutoOffsetReset;
using static Confluent.Kafka.CompressionType;
using static Confluent.Kafka.PartitionAssignmentStrategy;

namespace Krimson;

[PublicAPI]
public static class DefaultConfigs {
    public static readonly ClientConfig DefaultClientConfig = new() {
        BootstrapServers      = "localhost:9092",           // ready for docker
        ReconnectBackoffMaxMs = 10000 * 2,                  // 2x the default to allow for better handling of transient networks issues
        LogConnectionClose    = false,                      // documented as providing false alarms
        SocketKeepaliveEnable = true,                       // seems to help with network issues
        SaslMechanism         = SaslMechanism.Plain,        // default
        SecurityProtocol      = SecurityProtocol.Plaintext, // default
        BrokerAddressFamily   = BrokerAddressFamily.V4      // cause V6 will just create chaos (at least when using docker)
    };

    public static ConsumerConfig DefaultConsumerConfig =>
        new(new Dictionary<string, string>(DefaultClientConfig)) {
            EnableAutoOffsetStore       = false,             // because by default no one wants at-most-once but at-least-once processing
            EnableAutoCommit            = true,              // automatically and periodically commit positions/offsets
            AutoCommitIntervalMs        = 5000,              // default
            AutoOffsetReset             = Earliest,          // always read from the earliest position instead of the latest one
            EnablePartitionEof          = true,              // enable message marker to signal that the consumer has caught up
            SessionTimeoutMs            = 45000,             // allows for better handling of transient networks issues
            // HeartbeatIntervalMs         = 15000,             // 1/3 of session timeout
            PartitionAssignmentStrategy = CooperativeSticky  // enable incremental rebalancing to avoid stop-the-world rebalances
        };
    
    public static ConsumerConfig DefaultReaderConfig =>
        new(new Dictionary<string, string>(DefaultConsumerConfig)) {
            EnableAutoCommit            = false,
            PartitionAssignmentStrategy = RoundRobin,
            ReconnectBackoffMaxMs       = 10000,
            SocketKeepaliveEnable       = false,
        };
    
    public static ProducerConfig DefaultProducerConfig =>
        new(new Dictionary<string, string>(DefaultClientConfig)) {
            CompressionType      = Zstd, // cause it is that good
            CompressionLevel     = 6,    // can go from 1 to 12
            EnableIdempotence    = true, // there are no reasons to not want it enabled
            LingerMs             = 5,    // the default is now 5, funny
            DeliveryReportFields = "all" // default
        };

    public static SchemaRegistryConfig DefaultSchemaRegistryConfig =>
        new() {
            Url                        = "localhost:8081",              // ready for docker
            BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo
        };
}