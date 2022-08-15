using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Krimson.Connectors;

[PublicAPI]
public static class ServicesExtensions {
    public static IServiceCollection AddKrimsonPullSourceConnector<T>(this IServiceCollection services, TimeSpan? backoffTime = null) where T : PullSourceConnector {
        services.AddKrimsonReader();
        
        services.TryAddSingleton<DataSourceConsumer>();
        
        services.AddSingleton<T>();

        return backoffTime is null
            ? services.AddHostedService<PullSourceConnectorHost<T>>()
            : services.AddHostedService(ctx => new PullSourceConnectorHost<T>(ctx.GetRequiredService<T>(), ctx, backoffTime));
    }
}