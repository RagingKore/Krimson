using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace Krimson.State;

public class InMemoryStateStore : IStateStore {
    public InMemoryStateStore(MemoryCacheOptions cacheOptions, MemoryCacheEntryOptions entryOptions) {
        Cache        = new(new OptionsWrapper<MemoryCacheOptions>(cacheOptions));
        EntryOptions = entryOptions;
    }

    public InMemoryStateStore() {
        Cache        = new(new OptionsWrapper<MemoryCacheOptions>(new()));
        EntryOptions = new();
    }

    MemoryCacheEntryOptions EntryOptions { get; }
    MemoryCache             Cache        { get; }

    public ValueTask Set<T>(object key, T value, CancellationToken cancellationToken = default) {
        Cache.Set(key, value, EntryOptions);
        return ValueTask.CompletedTask;
    }

    public ValueTask<T?> Get<T>(object key, CancellationToken cancellationToken = default) {
        return ValueTask.FromResult(Cache.Get<T?>(key));
    }

    public ValueTask Delete<T>(object key, CancellationToken cancellationToken = default) {
        Cache.Remove(key);
        return ValueTask.CompletedTask;
    }

    public async ValueTask<T?> GetOrSet<T>(object key, Func<ValueTask<T>> factory, CancellationToken cancellationToken = default) => 
        await Cache.GetOrCreateAsync(key, async _ => await factory().ConfigureAwait(false));
}