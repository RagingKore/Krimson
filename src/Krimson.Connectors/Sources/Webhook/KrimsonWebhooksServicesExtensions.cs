namespace Krimson.Connectors;

public static class KrimsonWebhooksServicesExtensions {

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
                .AsImplementedInterfaces()
                .WithSingletonLifetime()
        );
}