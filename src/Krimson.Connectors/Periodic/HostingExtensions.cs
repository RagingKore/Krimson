namespace Krimson.Connectors;

[PublicAPI]
public static class ServicesExtensions {
    public static IServiceCollection AddKrimsonPeriodicSourceConnector<T>(this IServiceCollection services, TimeSpan? backoffTime = null) where T : PeriodicSourceConnector {
        services.AddSingleton<T>();

        return backoffTime is null
            ? services.AddHostedService<PeriodicSourceConnectorHost<T>>()
            : services.AddHostedService(ctx => new PeriodicSourceConnectorHost<T>(ctx.GetRequiredService<T>(), ctx, backoffTime));
    }
}