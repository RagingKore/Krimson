using Google.Protobuf.WellKnownTypes;

namespace Krimson.Connectors.Sources;

public partial class SourceRecord {
    public static readonly SourceRecord Empty = new() {
        Id        = string.Empty,
        Data      = new Struct(),
        Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.MinValue),
        Type      = string.Empty,
        Operation = SourceOperation.Unspecified,
        Source    = null
    };

    public static SourceRecord From(string id, string type, DateTimeOffset timestamp, Struct data, string? source = null) {
        return new SourceRecord {
            Id        = id,
            Type      = type,
            Data      = data,
            Timestamp = Timestamp.FromDateTimeOffset(timestamp),
            Operation = SourceOperation.Snapshot,
            Source    = source
        };
    }
}