using MongoDB.Driver;

namespace Krimson.State.MongoDB;

public class MongoStateStore : IStateStore {
    static readonly ReplaceOptions DefaultReplaceOptions = new ReplaceOptions {
        IsUpsert = true
    };
    
    static readonly TransactionOptions DefaultTransactionOptions = new TransactionOptions().With(
        ReadConcern.Local,
        ReadPreference.Primary,
        WriteConcern.W1
    );

    static readonly ClientSessionOptions DefaultClientSessionOptions =
        new() { DefaultTransactionOptions = DefaultTransactionOptions };

    public MongoStateStore(IMongoDatabase database, string? collectionName = null) {
        Database       = database;
        CollectionName = collectionName ?? "krimson-state";
    }

    IMongoDatabase Database       { get; }
    string?        CollectionName { get; }

    public async ValueTask Set<T>(object key, T value, CancellationToken cancellationToken = default) {
        if (key is null) throw new ArgumentNullException(nameof(key));

        var state = new MongoState<T>(key.ToString()!, value, DateTimeOffset.UtcNow);
        
        await Database
            .GetCollection<MongoState<T>>(CollectionName)
            .ReplaceOneAsync(x => x.Key == state.Key, state, DefaultReplaceOptions, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<T?> Get<T>(object key, CancellationToken cancellationToken = default) {
        if (key is null) throw new ArgumentNullException(nameof(key));

        var stateKey = key.ToString();
        
        return await Database
            .GetCollection<MongoState<T>>(CollectionName)
            .Find(x => x.Key == stateKey)
            .Project(x => x.Value)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask Delete<T>(object key, CancellationToken cancellationToken = default) {
        if (key is null) throw new ArgumentNullException(nameof(key));

        var stateKey = key.ToString();
        
        await Database
            .GetCollection<MongoState<T>>(CollectionName)
            .DeleteOneAsync(x => x.Key == stateKey, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<T?> GetOrSet<T>(object key, Func<ValueTask<T>> factory, CancellationToken cancellationToken = default) {
        using var session = await Database.Client
            .StartSessionAsync(DefaultClientSessionOptions, cancellationToken)
            .ConfigureAwait(false);
        
        return await session
            .WithTransactionAsync(Execute(), cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        Func<IClientSessionHandle, CancellationToken, Task<T>> Execute() =>
            async (_, ct) => {
                var value = await Get<T>(key, ct).ConfigureAwait(false);

                if (value is null) {
                    value = await factory().ConfigureAwait(false);
                    await Set(key, value, ct).ConfigureAwait(false);
                }
                    
                return value;
            };
    }
}