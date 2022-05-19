namespace Krimson.Kafka; 

public sealed record Partition(int Value) : IComparable<Partition>, IComparable {
    public static readonly Partition Any  = new(-1);
    public static readonly Partition None = new(0);

    public int CompareTo(object? obj) {
        if (ReferenceEquals(null, obj))
            return 1;

        if (ReferenceEquals(this, obj))
            return 0;

        return obj is Partition other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(Partition)}");
    }

    public int CompareTo(Partition? other) {
        if (ReferenceEquals(this, other))
            return 0;

        if (ReferenceEquals(null, other))
            return 1;

        return Value.CompareTo(other.Value);
    }

    public static implicit operator Partition(int partition) => new(partition);
    public static implicit operator int(Partition self)      => self.Value;
    public static implicit operator string(Partition self)   => self.Value.ToString();

    public override string ToString() => Value.ToString();

    public static bool operator <(Partition? left, Partition? right)  => Comparer<Partition>.Default.Compare(left, right) < 0;
    public static bool operator >(Partition? left, Partition? right)  => Comparer<Partition>.Default.Compare(left, right) > 0;
    public static bool operator <=(Partition? left, Partition? right) => Comparer<Partition>.Default.Compare(left, right) <= 0;
    public static bool operator >=(Partition? left, Partition? right) => Comparer<Partition>.Default.Compare(left, right) >= 0;
}