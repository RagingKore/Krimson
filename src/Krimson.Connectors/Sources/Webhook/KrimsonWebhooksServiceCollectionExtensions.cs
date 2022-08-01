namespace Krimson.Connectors.Source.Webhook;

public static class KrimsonWebhooksServiceCollectionExtensions {

    public static IServiceCollection AddKrimsonWebhook<T>(this IServiceCollection services)  where T : class, IKrimsonWebhook => 
        services.AddSingleton<IKrimsonWebhook, T>();

    public static IServiceCollection AddKrimsonWebhooks(this IServiceCollection services) =>
        services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<IKrimsonWebhook>()
                        .NotInNamespaceOf<IKrimsonWebhook>()
                )
                .As<IKrimsonWebhook>()
                .WithSingletonLifetime()
        );
}