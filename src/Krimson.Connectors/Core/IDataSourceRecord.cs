using Confluent.Kafka;

namespace Krimson.Connectors;

[PublicAPI]
public interface IDataSourceRecord {
    MessageKey                  Key              { get; set; }
    object                      Value            { get; set; }
    long                        EventTime        { get; set; }
    Dictionary<string, string?> Headers          { get; set; }
    string?                     DestinationTopic { get; set; }

    bool HasKey              { get; }
    bool HasDestinationTopic { get; }

    bool     Processed { get; }
    RecordId RecordId  { get; }

    void Ack(RecordId recordId);
    void Nak(ProduceException<byte[], object?> exception);
    
    /// <summary>
    /// Waits for the record to be processed assigning its id/position on success,
    /// and throwing the received producer exception otherwise
    /// </summary>
    Task EnsureProcessed();
}