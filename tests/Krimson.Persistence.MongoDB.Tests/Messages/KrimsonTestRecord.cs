namespace Krimson.Persistence.MongoDB.Tests.Messages;

public record KrimsonTestRecord {
    public required string         Id        { get; init; }
    public required int            Order     { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
}