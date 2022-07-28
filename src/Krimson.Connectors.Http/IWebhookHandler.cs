using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;

namespace Krimson.Connectors.Http;

public interface IWebhookHandler {
    public Task<bool> ValidatePayload(HttpContext context) {
        return Task.FromResult(true);
    }

    Task<IEnumerable<SourceRecord>> GetData(JsonNode node);
}