using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;

namespace Krimson.Processors;

static class KrimsonGapCheckServicesExtensions {
    public static IServiceCollection AddGapChecker(this IServiceCollection services) {
        services.AddSingleton<KrimsonSubscriptionGapChecker>(new KrimsonSubscriptionGapChecker());
        services.AddSingleton<IGapChecker>(sp => sp.GetRequiredService<KrimsonSubscriptionGapChecker>());

        return services;
    }
}

public interface IGapChecker {
    Task<bool> HasCaughtUp(string clientId);
}

class KrimsonSubscriptionGapChecker : IGapChecker {
    ConcurrentDictionary<string, Func<Task<bool>>> GapChecksByClientId { get; } = new ConcurrentDictionary<string, Func<Task<bool>>>();

    public void AddGapCheck(string clientId, GetSubscriptionGaps getSubscriptionGaps) {

        async Task<bool> GetGaps(GetSubscriptionGaps getGap) {
            var gap = await getGap();
            return gap.All(x => x.CaughtUp);
        }

        GapChecksByClientId.TryAdd(clientId, () => GetGaps(getSubscriptionGaps));
    }

    public async Task<bool> HasCaughtUp(string clientId) {
        var hasGapCheck = GapChecksByClientId.TryGetValue(clientId, out var gapCheck);
        if (!hasGapCheck) throw new InvalidOperationException($"No GapCheck found for ClientId {clientId}");
        if (gapCheck is null) throw new InvalidOperationException($"Gap check was null for ClientId {clientId}");

        return await gapCheck();
    }
}