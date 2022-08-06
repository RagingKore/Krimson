using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace Krimson.State.MongoDB;

public class MongoStateStore : IStateStore {
    static readonly ReplaceOptions DefaultReplaceOptions = new ReplaceOptions {
        IsUpsert = true
    };
    
    public MongoStateStore(IMongoDatabase database) => Database = database;

    IMongoDatabase Database { get; }

    public async ValueTask Set<T>(object key, T value, CancellationToken cancellationToken = default) {
        var state = new MongoState<T>(key.ToString()!, value, DateTimeOffset.UtcNow);
        
        await Database
            .GetCollection<MongoState<T>>("state")
            .ReplaceOneAsync(x => x.Key == state.Key, state, DefaultReplaceOptions, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<T?> Get<T>(object key, CancellationToken cancellationToken = default) {
        var stateKey = key.ToString();
        
        return await Database
            .GetCollection<MongoState<T>>("state")
            .Find(x => x.Key == stateKey)
            .Project(x => x.Value)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask Delete<T>(object key, CancellationToken cancellationToken = default) {
        var stateKey = key.ToString();
        
        await Database
            .GetCollection<MongoState<T>>("state")
            .DeleteOneAsync(x => x.Key == stateKey, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<T?> GetOrSet<T>(object key, Func<ValueTask<T>> factory, CancellationToken cancellationToken = default) {
        var stateKey = key.ToString()!;
      
        var exists = await Database
            .GetCollection<MongoState<T>>("state")
            .AsQueryable()
            .AnyAsync(x => x.Key == stateKey, cancellationToken)
            .ConfigureAwait(false);

        if (exists)
            return await Get<T>(key, cancellationToken).ConfigureAwait(false);

        var value = await factory().ConfigureAwait(false);
        var state = new MongoState<T>(stateKey, value, DateTimeOffset.UtcNow);

        await Set(key, state, cancellationToken);

        return value;
    }
}