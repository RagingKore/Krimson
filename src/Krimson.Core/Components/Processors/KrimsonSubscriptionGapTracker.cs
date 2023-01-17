using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;

namespace Krimson.Processors;

static class KrimsonGapCheckServicesExtensions {
    public static IServiceCollection AddKrimsonSubscriptionGapTracker(this IServiceCollection services) =>
        services.AddSingleton<IKrimsonSubscriptionGapTracker>(new KrimsonSubscriptionGapTracker());
}

public interface IKrimsonSubscriptionGapTracker {
    ValueTask<bool>                        HasCaughtUp(string clientId);
    IAsyncEnumerable<SubscriptionTopicGap> Gaps(string clientId);
}

class KrimsonSubscriptionGapTracker : IKrimsonSubscriptionGapTracker {
    ConcurrentDictionary<string, GetSubscriptionGaps> GapChecksByClientId { get; } = new();

    public void Register(IKrimsonProcessor processor) =>
        GapChecksByClientId.TryAdd(processor.ClientId, processor.GetSubscriptionGap);

    public async Task<IReadOnlyCollection<SubscriptionTopicGap>> GetGaps(string clientId) =>
        GapChecksByClientId.TryGetValue(clientId, out var getGaps)
            ? await getGaps().ConfigureAwait(false)
            : throw new InvalidOperationException($"ClientId {clientId} not found!");

    public ValueTask<bool> HasCaughtUp(string clientId) => Gaps(clientId).AllAsync(x => x.CaughtUp);

    public async IAsyncEnumerable<SubscriptionTopicGap> Gaps(string clientId) {
        foreach (var gap in await GetGaps(clientId)) yield return gap;
    }
}