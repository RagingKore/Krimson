namespace Krimson.Connectors.Checkpoints;

public record SourceCheckpoint(RecordId RecordId, long Timestamp) : IComparable<SourceCheckpoint>, IComparable {
    public static readonly SourceCheckpoint None = new(RecordId.None, DateTimeOffset.MinValue.ToUnixTimeMilliseconds());

    public static SourceCheckpoint From(SourceRecord processed) => new SourceCheckpoint(processed.RecordId, processed.EventTime);

    public override string ToString() => $"{RecordId} {DateTimeOffset.FromUnixTimeMilliseconds(Timestamp):O}";

    public int CompareTo(SourceCheckpoint? other) {
        if (ReferenceEquals(this, other)) return 0;
        if (ReferenceEquals(null, other)) return 1;

        return Timestamp.CompareTo(other.Timestamp);
    }

    public int CompareTo(object? obj) {
        if (ReferenceEquals(null, obj)) return 1;
        if (ReferenceEquals(this, obj)) return 0;

        return obj is SourceCheckpoint other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(SourceCheckpoint)}");
    }

    public static bool operator <(SourceCheckpoint? left, SourceCheckpoint? right)  => Comparer<SourceCheckpoint>.Default.Compare(left, right) < 0;
    public static bool operator >(SourceCheckpoint? left, SourceCheckpoint? right)  => Comparer<SourceCheckpoint>.Default.Compare(left, right) > 0;
    public static bool operator <=(SourceCheckpoint? left, SourceCheckpoint? right) => Comparer<SourceCheckpoint>.Default.Compare(left, right) <= 0;
    public static bool operator >=(SourceCheckpoint? left, SourceCheckpoint? right) => Comparer<SourceCheckpoint>.Default.Compare(left, right) >= 0;
}