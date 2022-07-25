using Google.Protobuf.WellKnownTypes;

namespace Krimson.Connectors;

public partial class SourceRecord {
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