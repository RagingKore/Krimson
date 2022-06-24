namespace Krimson;

[PublicAPI]
public sealed record TopicPartitionSequence(Topic Topic, Partition Partition, long Sequence) : IComparable<TopicPartitionSequence>, IComparable {
    static readonly long Beginning = -2L;
    static readonly long End       = -1L;
    static readonly long Stored    = -1000L;
    static readonly long Unset     = -1001L;

    public bool IsBeginning = Sequence == Beginning;
    public bool IsEnd       = Sequence == End;
    public bool IsStored    = Sequence == Stored;
    public bool IsUnset     = Sequence == Unset;

    public TopicPartition TopicPartition => new(Topic, Partition);

    public int CompareTo(object? obj) {
        if (ReferenceEquals(null, obj))
            return 1;

        if (ReferenceEquals(this, obj))
            return 0;

        return obj is TopicPartitionSequence other
            ? CompareTo(other)
            : throw new ArgumentException($"Object must be of type {nameof(TopicPartitionSequence)}");
    }

    public int CompareTo(TopicPartitionSequence? other) {
        if (ReferenceEquals(this, other))
            return 0;

        if (ReferenceEquals(null, other))
            return 1;

        var topicComparison = Topic.CompareTo(other.Topic);
        if (topicComparison != 0)
            return topicComparison;

        var partitionComparison = Partition.CompareTo(other.Partition);
        if (partitionComparison != 0)
            return partitionComparison;

        return Sequence.CompareTo(other.Sequence);
    }

    public bool Equals(TopicPartitionSequence? other) {
        if (ReferenceEquals(null, other))
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return Topic.Equals(other.Topic) && Partition.Equals(other.Partition) && Sequence.Equals(other.Sequence);
    }

    public override string ToString() => $"{TopicPartition} @ {Sequence}";

    public static implicit operator long(TopicPartitionSequence self) => self.Sequence;

    public override int GetHashCode() => HashCode.Combine(Topic, Partition, Sequence);

    public static bool operator <(TopicPartitionSequence? left, TopicPartitionSequence? right) =>
        Comparer<TopicPartitionSequence>.Default.Compare(left, right) < 0;

    public static bool operator >(TopicPartitionSequence? left, TopicPartitionSequence? right) =>
        Comparer<TopicPartitionSequence>.Default.Compare(left, right) > 0;

    public static bool operator <=(TopicPartitionSequence? left, TopicPartitionSequence? right) =>
        Comparer<TopicPartitionSequence>.Default.Compare(left, right) <= 0;

    public static bool operator >=(TopicPartitionSequence? left, TopicPartitionSequence? right) =>
        Comparer<TopicPartitionSequence>.Default.Compare(left, right) >= 0;
}