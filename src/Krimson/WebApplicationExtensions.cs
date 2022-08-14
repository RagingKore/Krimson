using Krimson.Connectors.Http;

namespace Krimson;

public static class WebApplicationExtensions {
    public static WebApplication UseKrimson(this WebApplication app) => app.UseKrimsonWebhookSourceConnectors();
}