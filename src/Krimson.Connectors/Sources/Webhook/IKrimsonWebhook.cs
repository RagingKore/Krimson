using System.Text.Json.Nodes;

namespace Krimson.Connectors;

public interface IKrimsonWebhook<TData> : ISourceConnector<KrimsonWebhookContext, TData> {
    string WebhookPath { get; }

    Task Subscribe(IServiceProvider services, CancellationToken cancellationToken);
    
    ValueTask<bool> Validate(KrimsonWebhookContext context);
}

public interface IKrimsonWebhook : IKrimsonWebhook<JsonNode> { }


[AttributeUsage(AttributeTargets.Class)]
public class WebhookPathAttribute : Attribute {
    public WebhookPathAttribute(string value) => Value = value;

    string Value { get; }

    public static implicit operator string(WebhookPathAttribute self) => self.Value;
}