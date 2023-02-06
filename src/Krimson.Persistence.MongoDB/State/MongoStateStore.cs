using Krimson.Persistence.State;
using MongoDB.Driver;

namespace Krimson.Persistence.MongoDB.State;

public class MongoStateStore : IStateStore {
    static readonly ReplaceOptions DefaultReplaceOptions = new() {
        IsUpsert = true
    };

    static readonly ClientSessionOptions DefaultClientSessionOptions =
        new() { DefaultTransactionOptions = new TransactionOptions().With(
            ReadConcern.Local,
            ReadPreference.Primary,
            WriteConcern.WMajority
        ) };

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

        var stateKey = key.ToString()!;
        
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

    public async ValueTask<T?> GetOrSet<T>(object key, Func<ValueTask<T?>> factory, CancellationToken cancellationToken = default) {
        using var session = await Database.Client
            .StartSessionAsync(DefaultClientSessionOptions, cancellationToken)
            .ConfigureAwait(false);
        
        return await session
            .WithTransactionAsync(Execute(), DefaultClientSessionOptions.DefaultTransactionOptions, cancellationToken)
            .ConfigureAwait(false);

        Func<IClientSessionHandle, CancellationToken, Task<T?>> Execute() =>
            async (_, ct) => {
                var value = await Get<T>(key, ct).ConfigureAwait(false);

                if (value is not null) return value;

                var newValue = await factory().ConfigureAwait(false);

                if (newValue is null) return value;

                await Set(key, newValue, ct).ConfigureAwait(false);

                return newValue;
            };
    }
}