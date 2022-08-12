using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Krimson.Connectors.Http;

public static class ServicesExtensions {
    public static IServiceCollection AddKrimsonWebhookSources(this IServiceCollection services) {
        return services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<WebhookSource>()
                        .NotInNamespaceOf<WebhookSource>()
                )
                .As<WebhookSource>()
                .WithSingletonLifetime()
        );
    }
}

public static class WebApplicationExtensions {
    public static WebApplication UseKrimsonWebhookSources(this WebApplication app) {
        foreach (var source in app.Services.GetServices<WebhookSource>())
            app.MapPost(
                source.WebhookPath, async http => await source.Execute(new(http)).ConfigureAwait(false)
            );

        return app;
    }
}

// public static class ServicesExtensions {
//     public static IServiceCollection AddKrimsonWebhookSourceConnector(this IServiceCollection services, WebhookSourceOptions? options = null) {
//         if (options is null)
//             services.TryAddSingleton(ctx => new WebhookSourceOptions(ctx.GetRequiredService<KrimsonProducer>()));
//         else
//             services.AddSingleton(options);
//
//         services.TryAddSingleton<WebhookSource>();
//         
//         return services.Scan(
//             scan => scan.FromApplicationDependencies()
//                 .AddClasses(
//                     classes => classes
//                         .AssignableTo<WebhookSource>()
//                         .NotInNamespaceOf<WebhookSource>()
//                 )
//                 .As<WebhookSource>()
//                 .WithSingletonLifetime()
//         );
//     }
// }

// public static class WebApplicationExtensions {
//     public static WebApplication UseKrimsonWebhookSourceConnector(this WebApplication app) {
//         foreach (var module in app.Services.GetServices<WebhookSource>())
//             app.MapPost(
//                 module.WebhookPath, async http => {
//                     await http.RequestServices
//                         .GetRequiredService<WebhookSource>()
//                         .Execute(module, new(http))
//                         .ConfigureAwait(false);
//                 }
//             );
//
//         return app;
//     }
// }