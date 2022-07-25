using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Connectors;

[PublicAPI]
public static class ServiceCollectionExtensions {
    public static IServiceCollection AddPullSourceConnector<T>(this IServiceCollection services) where T : PullSourceConnector =>
        services
            .AddSingleton<T>()
            .AddHostedService<BackgroundPullSourceConnectorService<T>>();

    public static IServiceCollection AddPeriodicSourceConnector<T>(this IServiceCollection services, Action<PeriodicSourceConnectorOptions>? configure = null)
        where T : PullSourceConnector =>
        services
            .AddSingleton(new PeriodicSourceConnectorOptions().With(options => configure?.Invoke(options)))
            .AddPullSourceConnector<T>();
}