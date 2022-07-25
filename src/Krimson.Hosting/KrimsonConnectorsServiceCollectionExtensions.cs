using Krimson.Connectors;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class KrimsonConnectorsServiceCollectionExtensions {
    static IServiceCollection AddKrimsonPullSourceConnector<T>(this IServiceCollection services) where T : PullSourceConnector =>
        services
            .AddSingleton<T>()
            .AddHostedService<BackgroundPullSourceConnectorService<T>>();

    public static IServiceCollection AddKrimsonPeriodicSourceConnector<T>(this IServiceCollection services, Action<PeriodicSourceConnectorOptions>? configure = null)
        where T : PullSourceConnector =>
        services
            .AddSingleton(new PeriodicSourceConnectorOptions().With(options => configure?.Invoke(options)))
            .AddKrimsonPullSourceConnector<T>();
}