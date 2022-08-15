using Microsoft.Extensions.DependencyInjection.Extensions;
using Scrutor;

namespace Krimson.Connectors.Http;

public static class ServicesExtensions {
    public static IServiceCollection AddKrimsonWebhookSourceConnectors(this IServiceCollection services) {
        services.AddKrimsonReader();
        
        services.AddHostedService<DataSourceConsumer>();
        
        services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<WebhookSourceConnector>()
                        .NotInNamespaceOf<WebhookSourceConnector>()
                )
                .UsingRegistrationStrategy(RegistrationStrategy.Skip)
                .AsSelfWithInterfaces()
                .WithSingletonLifetime()
        );

        return services;
    }

    public static IServiceCollection AddKrimsonWebhookSourceConnector<T>(this IServiceCollection services) where T: WebhookSourceConnector {
        services.AddKrimsonReader();
        
        services.AddHostedService<DataSourceConsumer>();

        services.Scan(scan => scan
            .FromAssemblyOf<T>()
            .AddClasses(classes => classes.AssignableTo<WebhookSourceConnector>())
            .UsingRegistrationStrategy(RegistrationStrategy.Skip)
            .AsSelfWithInterfaces()
            .WithSingletonLifetime()
        );

        return services;
    }
}

public static class WebApplicationExtensions {
    public static WebApplication UseKrimsonWebhookSourceConnectors(this WebApplication app) {
        foreach (var source in app.Services.GetServices<WebhookSourceConnector>()) {
            var webhookPath = GetWebhookPath(source.GetType());
            
            app.MapPost(webhookPath, async http => await source.Process(new(http)).ConfigureAwait(false));
        }
        
        return app;
        
        static string GetWebhookPath(Type type) => 
            (WebhookPathAttribute?)Attribute.GetCustomAttribute(type, typeof(WebhookPathAttribute)) ?? "";
    }

    public static WebApplication UseKrimsonWebhooks(this WebApplication app) => app.UseKrimsonWebhookSourceConnectors();
}