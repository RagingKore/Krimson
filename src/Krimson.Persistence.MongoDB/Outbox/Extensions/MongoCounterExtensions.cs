using MongoDB.Driver;
using static MongoDB.Driver.Builders<Krimson.Persistence.MongoDB.Outbox.MongoCounterExtensions.Counter>;

namespace Krimson.Persistence.MongoDB.Outbox;

public static class MongoCounterExtensions {
    static readonly FindOneAndUpdateOptions<Counter> DefaultCounterOptions = new() {
        ReturnDocument = ReturnDocument.After,
        Projection     = Projection.Include(x => x.Value),
        IsUpsert       = true
    };

    // ReSharper disable once ClassNeverInstantiated.Global
    internal record Counter(string Id, long Value = -1);

    public static async Task<long> GetNextNumber(this IMongoDatabase database, IClientSessionHandle session, string key, int increment = 1, string collectionName = "krimson-counters") {
        var result = await database
            .GetCollection<Counter>(collectionName)
            .FindOneAndUpdateAsync(session, Filter.Eq(x => x.Id, key), Update.Inc(x => x.Value, increment), DefaultCounterOptions);

        return result.Value;
    }

    public static async Task<long> GetActualNumber(this IMongoDatabase database, IClientSessionHandle session, string key, string collectionName = "krimson-counters") {
        var result = await database
            .GetCollection<Counter>(collectionName)
            .Find(session, Filter.Eq(x => x.Id, key))
            .FirstOrDefaultAsync();

        return result?.Value ?? -1;
    }

    public static async Task<long> GetActualNumber(this IMongoDatabase database, string key, string collectionName = "krimson-counters") {
        var result = await database
            .GetCollection<Counter>(collectionName)
            .Find(Filter.Eq(x => x.Id, key))
            .FirstOrDefaultAsync();

        return result?.Value ?? -1;
    }
}