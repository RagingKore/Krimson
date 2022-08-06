using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Serilog;

namespace Krimson.State;

public class DistributedStateStore : IStateStore {
    static readonly ILogger Log = Serilog.Log.ForContext<DistributedStateStore>();
    
    public DistributedStateStore(IDistributedCache cache, DistributedCacheEntryOptions? entryOptions = null) {
        Cache        = cache;
        EntryOptions = entryOptions ?? new DistributedCacheEntryOptions();
    }

    IDistributedCache            Cache        { get; }
    DistributedCacheEntryOptions EntryOptions { get; }

    public async ValueTask Set<T>(object key, T value, CancellationToken cancellationToken = default) {
        await Cache
            .SetAsync(key.ToString(), JsonSerializer.SerializeToUtf8Bytes(value), EntryOptions, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<T?> Get<T>(object key, CancellationToken cancellationToken = default) {
        var bytes = await Cache
            .GetAsync(key.ToString(), cancellationToken)
            .ConfigureAwait(false);

        return JsonSerializer.Deserialize<T>(bytes);
    }

    public async ValueTask Delete<T>(object key, CancellationToken cancellationToken = default) {
        await Cache
            .RemoveAsync(key.ToString(), cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<T?> GetOrSet<T>(object key, Func<ValueTask<T>> factory, CancellationToken cancellationToken = default) {
        var cacheKey = key.ToString();
        
        try {
            var data = await Cache
                .GetAsync(cacheKey, cancellationToken)
                .ConfigureAwait(false);

            if (data.Length > 0)
                return JsonSerializer.Deserialize<T>(data);
        }
        catch (Exception ex) {
            Log.Warning(ex, "Failed to get value: {Key}", key);
        }

        var value = await factory().ConfigureAwait(false);

        try {
            await Cache
                .SetAsync(cacheKey, JsonSerializer.SerializeToUtf8Bytes(value), EntryOptions, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) {
            Log.Warning(ex, "Failed to set value: {Key}", key);
        }

        return value;
    }
}