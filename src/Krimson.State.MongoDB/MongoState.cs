namespace Krimson.State.MongoDB;

public record MongoState<T>(string Key, T Value, DateTimeOffset Timestamp);