namespace Krimson.Connectors;

[PublicAPI]
public static class KrimsonPeriodicSourceConnectorsServiceCollectionExtensions {
    public static IServiceCollection AddKrimsonPeriodicSourceConnector<T>(this IServiceCollection services)
        where T : KrimsonPeriodicSourceConnector =>
        services
            .AddSingleton<T>()
            .AddHostedService<KrimsonPeriodicSourceConnectorService<T>>();
}