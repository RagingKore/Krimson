// ReSharper disable CheckNamespace

using Krimson.Connectors.Http;

namespace Krimson.Connectors;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    public static KrimsonBuilder AddPullSourceConnector<T>(this KrimsonBuilder builder, TimeSpan? backoffTime = null) where T : PullSourceConnector {
        builder.Services.AddKrimsonPullSourceConnector<T>(backoffTime);
        return builder;
    }

    public static KrimsonBuilder AddWebhookSourceConnector<T>(this KrimsonBuilder builder) where T : WebhookSourceConnector {
        builder.Services.AddKrimsonWebhookSourceConnector<T>();
        return builder;
    }

    public static KrimsonBuilder AddWebhookSourceConnectors(this KrimsonBuilder builder) {
        builder.Services.AddKrimsonWebhookSourceConnectors();
        return builder;
    }
}