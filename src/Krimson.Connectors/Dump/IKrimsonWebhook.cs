// using System.Text.Json.Nodes;
//
// namespace Krimson.Connectors;
//
// public interface IKrimsonWebhook<TData> : ISourceConnector<KrimsonWebhookContext, TData> {
//     string WebhookPath { get; }
//
//     ValueTask<bool> Validate(KrimsonWebhookContext context);
// }
//
// public interface IKrimsonWebhook : IKrimsonWebhook<JsonNode> { }