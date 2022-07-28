using System.Text.Json.Nodes;
using Krimson.Producers;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Krimson.Connectors.Http;

public static class WebApplicationExtensions {
    public static WebApplication AddWebhookConnector(this WebApplication webApplication, string pathtowebhook) {
        webApplication.MapPost(
            pathtowebhook, async (HttpContext context, KrimsonProducer producer, JsonNode node, IWebhookHandler handler) => {
                var shouldContinue = await handler.ValidatePayload(context);
                if (!shouldContinue) {
                    return Results.BadRequest();
                }

                var results = await handler.GetData(node);
                foreach (var sourceRecord in results) {
                    await DispatchRecord(producer, sourceRecord);
                }

                return Results.Ok();
            }
        );

        async ValueTask<ProducerResult> DispatchRecord(KrimsonProducer producer, SourceRecord record) =>
            await producer.Produce(record, record.Id).ConfigureAwait(false);

        return webApplication;
    }
}