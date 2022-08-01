using Google.Protobuf.WellKnownTypes;
using Krimson.Connectors.Sources;

namespace Krimson.Connectors;

public record Checkpoint(RecordId RecordId, Timestamp Timestamp) {
    public static readonly Checkpoint None = new(RecordId.None, Timestamp.FromDateTimeOffset(DateTimeOffset.MinValue));
    
    public static Checkpoint From(ProcessedSourceRecord processed) => new Checkpoint(processed.RecordId, processed.SourceRecord.Timestamp);

    public override string ToString() => $"{RecordId} {Timestamp.ToDateTimeOffset():O}";
}