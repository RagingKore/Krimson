using Confluent.Kafka;

namespace Krimson.Connectors;

public record Checkpoint(RecordId RecordId, Timestamp Timestamp) {
    public static readonly Checkpoint None = new(RecordId.None, Timestamp.Default);
    
    public static Checkpoint From(ProcessedSourceRecord processed) => new Checkpoint(processed.RecordId, processed.SourceRecord.Timestamp);

    public override string ToString() => $"{RecordId} {DateTimeOffset.FromUnixTimeMilliseconds(Timestamp.UnixTimestampMs):O}";
}