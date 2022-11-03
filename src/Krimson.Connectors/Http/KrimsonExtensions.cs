using Krimson.Connectors.Http;

namespace Krimson.Connectors;

public static partial class ServiceCollectionExtensions {
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

[PublicAPI]
public static class WebApplicationExtensions {
    public static WebApplication UseKrimsonWebhookSourceConnectors(this WebApplication app) {
        foreach (var connector in app.Services.GetServices<WebhookSourceConnector>()) {
            var webhookPath = WebhookPathAttribute.GetValue(connector);
            
            if (string.IsNullOrWhiteSpace(webhookPath))
                throw new($"{connector.GetType().Name} webhook path is missing or undefined");
            
            app.MapPost(webhookPath, async http => await connector.Process(new(http)).ConfigureAwait(false));
        }
        
        return app;
    }

    public static WebApplication UseKrimsonWebhooks(this WebApplication app) => 
        app.UseKrimsonWebhookSourceConnectors();
}

[PublicAPI]
public static partial class KrimsonBuilderExtensions {
    public static KrimsonBuilder AddWebhookSourceConnector<T>(this KrimsonBuilder builder) where T : WebhookSourceConnector {
        builder.Services.AddKrimsonWebhookSourceConnector<T>();
        return builder;
    }

    public static KrimsonBuilder AddWebhookSourceConnectors(this KrimsonBuilder builder) {
        builder.Services.AddKrimsonWebhookSourceConnectors();
        return builder;
    }
    
    public static KrimsonBuilder AddWebhookSource<T>(this KrimsonBuilder builder) where T : WebhookSourceConnector {
        builder.Services.AddKrimsonWebhookSourceConnector<T>();
        return builder;
    }

    public static KrimsonBuilder AddWebhookSources(this KrimsonBuilder builder) {
        builder.Services.AddKrimsonWebhookSourceConnectors();
        return builder;
    }
}