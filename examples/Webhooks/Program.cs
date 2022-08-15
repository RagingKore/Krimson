using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson;
using Krimson.Connectors;
using Krimson.Connectors.Http;

var builder = WebApplication.CreateBuilder(args);

builder.UseKrimson()
    .AddWebhookSourceConnector<PowerMetersWebhook>() // or simply call .AddWebhookSources() to automatically scan and register all webhooks
    .AddReader(rdr => rdr.ClientId("my_app_name"))
    .AddProducer(pdr => pdr.ClientId("my_app_name").Topic("power-company.meters"));

var app = builder.Build()
    .UseKrimsonWebhooks();

app.Run();


[WebhookPath("/meters")]
class PowerMetersWebhook : WebhookSourceConnector {
    public override ValueTask<bool> OnValidate(WebhookSourceContext context) {
        var header = context.Request.Headers["X-Signature"].ToString();
        return ValueTask.FromResult(header == "this_is_fine");
    }
    
    public override async IAsyncEnumerable<SourceRecord> ParseRecords(WebhookSourceContext context) {
        var result = await context.Request.ReadFromJsonAsync<JsonNode>().ConfigureAwait(false);
        
        foreach (var item in result?.AsArray() ?? new JsonArray())
            yield return ParseRecord(item!);
        
        static SourceRecord ParseRecord(JsonNode node) {
            var key       = node["id"]!.GetValue<string>();
            var eventTime = node["last_modified"]!.GetValue<DateTimeOffset>().ToUnixTimeMilliseconds();
            var data      = Struct.Parser.ParseJson(node.ToJsonString());
        
            return new() {
                Key              = key,
                Value            = data,
                EventTime        = eventTime,
                DestinationTopic = null,
                Headers          = new () { { "source", "power-meters" } }
            };
        }
    }
}