namespace Krimson.Kafka; 

public record TopicPartition(Topic Topic, Partition Partition)
    : IComparable<TopicPartition>, IComparable {
    public int CompareTo(object? obj) {
        if (ReferenceEquals(null, obj))
            return 1;

        if (ReferenceEquals(this, obj))
            return 0;

        return obj is TopicPartition other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(TopicPartition)}");
    }

    public int CompareTo(TopicPartition? other) {
        if (ReferenceEquals(this, other))
            return 0;

        if (ReferenceEquals(null, other))
            return 1;

        var topicComparison = Topic.CompareTo(other.Topic);
        if (topicComparison != 0)
            return topicComparison;

        return Partition.CompareTo(other.Partition);
    }

    public virtual bool Equals(TopicPartition? other) {
        if (ReferenceEquals(null, other))
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return Topic.Equals(other.Topic)
            && Partition.Equals(other.Partition);
    }

    public override string ToString() => $"{Topic} [{Partition}]";

    public static implicit operator TopicPartition(Topic topic) => new(topic, -1);
    public static implicit operator string(TopicPartition self) => self.ToString();
    public static implicit operator long(TopicPartition self)   => self.Partition.Value;

    public override int GetHashCode() => HashCode.Combine(Topic, Partition);

    public static bool operator <(TopicPartition? left, TopicPartition? right)  => Comparer<TopicPartition>.Default.Compare(left, right) < 0;
    public static bool operator >(TopicPartition? left, TopicPartition? right)  => Comparer<TopicPartition>.Default.Compare(left, right) > 0;
    public static bool operator <=(TopicPartition? left, TopicPartition? right) => Comparer<TopicPartition>.Default.Compare(left, right) <= 0;
    public static bool operator >=(TopicPartition? left, TopicPartition? right) => Comparer<TopicPartition>.Default.Compare(left, right) >= 0;
}