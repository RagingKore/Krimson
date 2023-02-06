using MongoDB.Driver;
using static MongoDB.Driver.Builders<Krimson.Persistence.MongoDB.Outbox.MongoSequenceNumberExtensions.Counter>;

namespace Krimson.Persistence.MongoDB.Outbox;

public static class MongoSequenceNumberExtensions {
    static readonly FindOneAndUpdateOptions<Counter> DefaultCounterOptions = new() {
        ReturnDocument = ReturnDocument.After,
        Projection     = Projection.Include(x => x.Seq),
        IsUpsert       = true
    };

    static readonly FindOneAndUpdateOptions<Counter> DefaultCounterExpectedOptions = new() {
        ReturnDocument = ReturnDocument.After,
        Projection     = Projection.Include(x => x.Seq),
        IsUpsert       = false
    };

    // ReSharper disable once ClassNeverInstantiated.Global
    internal record Counter(string Id, long Seq = -1);

    // public static async Task<long> GetNextSequence(this IMongoDatabase database, string key, long? expectedSequenceNumber = null, int increment = 1, string collectionName = "krimson-counters") {
    //     var filter = expectedSequenceNumber is null
    //         ? Filter.Eq(x => x.Id, key)
    //         : Filter.And(
    //             Filter.Eq(x => x.Id, key),
    //             Filter.Eq(x => x.Seq, expectedSequenceNumber)
    //         );
    //
    //     var options = expectedSequenceNumber.HasValue ? DefaultCounterExpectedOptions : DefaultCounterOptions;
    //
    //     var result = await database.GetCollection<Counter>(collectionName)
    //         .FindOneAndUpdateAsync(filter, Update.Inc(x => x.Seq, increment), options);
    //
    //     if (expectedSequenceNumber.HasValue && (result is null || result.Seq <= 0)) {
    //         throw new InvalidOperationException("Concurrency exception");
    //     }
    //
    //     return result.Seq;
    // }

    public static async Task<long> GetNextSequenceNumber(this IMongoDatabase database, IClientSessionHandle session, string key, int increment = 1, string collectionName = "krimson-counters") {
        var result = await database
            .GetCollection<Counter>(collectionName)
            .FindOneAndUpdateAsync(session, Filter.Eq(x => x.Id, key), Update.Inc(x => x.Seq, increment), DefaultCounterOptions);

        return result.Seq;
    }

    public static async Task<long> GetCurrentSequenceNumber(this IMongoDatabase database, IClientSessionHandle session, string key, string collectionName = "krimson-counters") {
        var result = await database
            .GetCollection<Counter>(collectionName)
            .Find(session, Filter.Eq(x => x.Id, key))
            .FirstOrDefaultAsync();

        return result?.Seq ?? -1;
    }

    public static async Task<long> GetCurrentSequenceNumber(this IMongoDatabase database, string key, string collectionName = "krimson-counters") {
        var result = await database
            .GetCollection<Counter>(collectionName)
            .Find(Filter.Eq(x => x.Id, key))
            .FirstOrDefaultAsync();

        return result?.Seq ?? -1;
    }

    // public static async Task<long> GetNextSequence(this IMongoDatabase database, IClientSessionHandle session, string key, int increment = 1, long? expectedSequenceNumber = null, string collectionName = "krimson-counters") {
    //     var filter = expectedSequenceNumber is null
    //         ? Filter.Eq(x => x.Id, key)
    //         : Filter.And(
    //             Filter.Eq(x => x.Id, key),
    //             Filter.Eq(x => x.Seq, expectedSequenceNumber)
    //         );
    //
    //     var options = expectedSequenceNumber.HasValue ? DefaultCounterExpectedOptions : DefaultCounterOptions;
    //
    //     var result = await database.GetCollection<Counter>(collectionName)
    //         .FindOneAndUpdateAsync(session, filter, Update.Inc(x => x.Seq, increment), options);
    //
    //     if (expectedSequenceNumber.HasValue && (result is null || result.Seq <= 0)) {
    //         throw new InvalidOperationException("Concurrency exception");
    //     }
    //
    //     return result.Seq;
    // }

    //
    // static readonly FindOneAndUpdateOptions<BsonDocument> DefaultOptions1 = new FindOneAndUpdateOptions<BsonDocument> {
    //     ReturnDocument = ReturnDocument.After,
    //     Projection     = new BsonDocument("seq", 1),
    //     IsUpsert       = true
    // };
    //
    // public static async Task<long> GetNextSequenceAsync(this IMongoDatabase database, string key, int increment = 1, string collectionName = "krimson-counters") {
    //     var result = await database
    //         .GetCollection<BsonDocument>(collectionName)
    //         .FindOneAndUpdateAsync(
    //             new BsonDocument("_id", key),
    //             new BsonDocument("$inc", new BsonDocument("seq", increment)),
    //             DefaultOptions1
    //         );
    //
    //     return result["seq"].AsInt64;
    // }
}