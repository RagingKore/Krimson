namespace Krimson.Connectors.Sources;

public static class KrimsonWebhooksWebApplicationExtensions {
    static WebApplication UseKrimsonWebhook(this WebApplication app, IKrimsonWebhook webhook) {
        app.MapPost(webhook.WebhookPath, async http => {
            await webhook
                .Execute(new KrimsonWebhookContext(http))
                .ConfigureAwait(false);
        });
        
        return app;
    }
    
    public static WebApplication UseKrimsonWebhooks(this WebApplication app) {
        foreach (var webhook in app.Services.GetServices<IKrimsonWebhook>()) 
            app.UseKrimsonWebhook(webhook);

        return app;
    }
    
    public static WebApplication UseKrimsonWebhook<T>(this WebApplication app) where T : IKrimsonWebhook {
        var webhook = app.Services.GetRequiredService<T>();
        return app.UseKrimsonWebhook(webhook);
    }
    
    // public static WebApplication UseKrimsonWebhooks(this WebApplication app) {
    //     var connectors = app.Services.GetServices<KrimsonWebhook>();
    //
    //     foreach (var connector in connectors) {
    //         app.MapPost(connector.WebhookPath, async http => {
    //             await connector
    //                 .Execute(new KrimsonWebhookContext(http))
    //                 .ConfigureAwait(false);
    //         });
    //     }
    //     
    //     return app;
    // }

    // public static WebApplication UseKrimsonWebhook<T>(this WebApplication app) where T : IKrimsonWebhook {
    //     var webhook     = app.Services.GetRequiredService<KrimsonWebhook>();
    //     var webhookPath = webhook.WebhookPath;
    //     var route       = (RouteAttribute?) Attribute.GetCustomAttribute(webhook.GetType(), typeof (RouteAttribute));
    //
    //     if (route is not null) webhookPath = route.Template;
    //     
    //     app.MapPost(webhookPath, async http => {
    //         await webhook
    //             .Execute(new KrimsonWebhookContext(http))
    //             .ConfigureAwait(false);
    //     });
    //     
    //     return app;
    // }
}