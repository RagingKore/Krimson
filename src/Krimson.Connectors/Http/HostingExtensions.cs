namespace Krimson.Connectors.Http;

public static class ServicesExtensions {
    public static IServiceCollection AddKrimsonWebhookSourceConnectors(this IServiceCollection services) {
        services.AddKrimsonReader();

        services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<WebhookSourceConnector>()
                        .NotInNamespaceOf<WebhookSourceConnector>()
                )
                .As<WebhookSourceConnector>()
                .AsImplementedInterfaces()
                .AsSelf()
                .WithSingletonLifetime()
        );

        return services;
    }
    
    public static IServiceCollection AddKrimsonWebhookSourceConnector<T>(this IServiceCollection services) where T: WebhookSourceConnector {
        services.AddKrimsonReader();

        services.Scan(
            scan => scan.FromAssemblyOf<T>()
                .AddClasses(classes => classes.AssignableTo<WebhookSourceConnector>())
                .As<WebhookSourceConnector>()
                .AsImplementedInterfaces()
                .AsSelf()
                .WithSingletonLifetime()
        );
    
        return services;
    }
}

public static class WebApplicationExtensions {
    public static WebApplication UseKrimsonWebhookSourceConnectors(this WebApplication app) {
        foreach (var connector in app.Services.GetServices<WebhookSourceConnector>()) {
            var webhookPath = GetWebhookPath(connector.GetType());
            
            if (string.IsNullOrWhiteSpace(webhookPath))
                throw new($"{connector.GetType().Name} webhook path is missing or undefined");
            
            app.MapPost(webhookPath, async http => await connector.Process(new(http)).ConfigureAwait(false));
        }
        
        return app;
        
        static string GetWebhookPath(Type type) => 
            (WebhookPathAttribute?)Attribute.GetCustomAttribute(type, typeof(WebhookPathAttribute)) ?? "";
    }

    public static WebApplication UseKrimsonWebhooks(this WebApplication app) => app.UseKrimsonWebhookSourceConnectors();
}