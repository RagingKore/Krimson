using Confluent.Kafka;
using static System.String;

namespace Krimson;

[PublicAPI]
public record RecordId : IComparable<RecordId>, IComparable {
    public static readonly RecordId None = new(new TopicPartitionOffset(Empty, Confluent.Kafka.Partition.Any, Confluent.Kafka.Offset.Unset));
   
    public RecordId(TopicPartitionOffset position) {
        Position  = position;
        Topic     = Position.Topic;
        Partition = Position.Partition.Value;
        Offset    = Position.Offset.Value;
        Value     = $"{Topic.ToLower()}:{Partition}:{Offset}";
    }

    public string               Value    { get; }
    public TopicPartitionOffset Position { get; }

    internal string Topic     { get; }
    internal int    Partition { get; }
    internal long   Offset    { get; }

    public static RecordId From(TopicPartitionOffset position) => new RecordId(position);

    public static RecordId From(string value) {
        try {
            var parts    = value.Split(":");
            var position = new TopicPartitionOffset(parts[0].ToLower(), int.Parse(parts[1]), int.Parse(parts[2]));
            return new(position);
        }
        catch (Exception ex) {
            throw new($"Failed to parse record id from: {value}", ex);
        }
    }
    
    public override string ToString() => Value;

    public static implicit operator string(RecordId self)  => self.Value;
    public static implicit operator RecordId(string value) => From(value);

    public static implicit operator TopicPartitionOffset(RecordId self)     => self.Position;
    public static implicit operator RecordId(TopicPartitionOffset position) => From(position);

    public int CompareTo(RecordId? other) {
        if (ReferenceEquals(this, other)) return 0;
        if (ReferenceEquals(null, other)) return 1;

        var topicComparison = Compare(Topic, other.Topic, StringComparison.OrdinalIgnoreCase);
        if (topicComparison != 0)
            return topicComparison;

        var partitionComparison = Partition.CompareTo(other.Partition);
        if (partitionComparison != 0)
            return partitionComparison;

        return Offset.CompareTo(other.Offset);
    }

    public int CompareTo(object? obj) {
        if (ReferenceEquals(null, obj)) return 1;
        if (ReferenceEquals(this, obj)) return 0;

        return obj is RecordId other 
            ? CompareTo(other) 
            : throw new ArgumentException($"Object must be of type {nameof(RecordId)}");
    }

    public static bool operator <(RecordId? left, RecordId? right)  => Comparer<RecordId>.Default.Compare(left, right) < 0;
    public static bool operator >(RecordId? left, RecordId? right)  => Comparer<RecordId>.Default.Compare(left, right) > 0;
    public static bool operator <=(RecordId? left, RecordId? right) => Comparer<RecordId>.Default.Compare(left, right) <= 0;
    public static bool operator >=(RecordId? left, RecordId? right) => Comparer<RecordId>.Default.Compare(left, right) >= 0;
}