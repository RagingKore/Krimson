namespace Krimson.Connectors;

[PublicAPI]
public static class ServicesExtensions {
    public static IServiceCollection AddKrimsonPullSourceConnector<T>(this IServiceCollection services, TimeSpan? backoffTime = null) where T : PullSourceConnector {
        services.AddKrimsonReader();

        services.Scan(
            scan => scan
                .FromAssemblyOf<T>()
                .AddClasses(classes => classes.AssignableTo<PullSourceConnector>())
                .AsImplementedInterfaces()
                .AsSelf()
                .WithSingletonLifetime()
        );

        return backoffTime is null
            ? services.AddHostedService<PullSourceConnectorHost<T>>()
            : services.AddHostedService(ctx => new PullSourceConnectorHost<T>(ctx.GetRequiredService<T>(), ctx, backoffTime));
    }
}