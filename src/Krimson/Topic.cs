namespace Krimson.Kafka; 

public record Topic(string Name)
    : IComparable<Topic>, IComparable {
    public int CompareTo(object? obj) {
        if (ReferenceEquals(null, obj))
            return 1;

        if (ReferenceEquals(this, obj))
            return 0;

        return obj is Topic other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(Topic)}");
    }

    public int CompareTo(Topic? other) {
        if (ReferenceEquals(this, other))
            return 0;

        if (ReferenceEquals(null, other))
            return 1;

        return string.Compare(Name, other.Name, StringComparison.Ordinal);
    }

    public override string ToString() => Name;

    public static implicit operator Topic(string value) => new(value);
    public static implicit operator string(Topic self)  => self.Name;

    public static bool operator <(Topic? left, Topic? right) => Comparer<Topic>.Default.Compare(left, right) < 0;
    public static bool operator >(Topic? left, Topic? right) => Comparer<Topic>.Default.Compare(left, right) > 0;
    public static bool operator <=(Topic? left, Topic? right) => Comparer<Topic>.Default.Compare(left, right) <= 0;
    public static bool operator >=(Topic? left, Topic? right) => Comparer<Topic>.Default.Compare(left, right) >= 0;
}