using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson;
using Krimson.Connectors;
using Krimson.Connectors.Http;

var builder = WebApplication.CreateBuilder(args);

builder.UseKrimson()
    .AddProducer(
        pdr => pdr
            .ClientId("my_app_name")
            .Topic("foo.bar.baz")
    )
    .AddWebhookSourceConnectors(); // or simply call .AddWebhooks() to automatically scan and register all webhooks

var app = builder.Build();

app.UseKrimson();

app.MapGet("/", () => "Hello World!");

app.Run();


[WebhookPath("/meters")]
class PowerMetersWebhookEndpoint : WebhookSourceConnector {
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