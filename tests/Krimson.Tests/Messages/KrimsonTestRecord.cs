namespace Krimson.Tests.Messages;

public record KrimsonTestRecord {
    public string         Id        { get; init; }
    public int            Order     { get; init; }
    public DateTimeOffset Timestamp { get; init; }
}