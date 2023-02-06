namespace Krimson.Persistence.MongoDB.State;

public record MongoState<T>(string Key, T Value, DateTimeOffset Timestamp);