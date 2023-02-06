using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson;
using Krimson.Connectors;
using Krimson.Connectors.AspNet;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddKrimson("my_app_name")
    .AddReader()
    .AddProducer(pdr => pdr.Topic("power-company.meters"))
    .AddWebhookSourceConnector<PowerMetersWebhook>(); // or simply call .AddWebhookSources() to automatically scan and register all webhooks

var app = builder.Build()
    .UseKrimsonWebhooks();

app.Run();

[WebhookPath("/meters")]
class PowerMetersWebhook : WebhookSourceConnector {
    public PowerMetersWebhook() {
        OnValidate(
            ctx => {
                var header = ctx.Request.Headers["X-Signature"].ToString();
                return ValueTask.FromResult(header == "this_is_fine");
            }
        );
    }

    public override async IAsyncEnumerable<SourceRecord> ParseRecords(WebhookSourceContext context) {
        var result = await context.Request.ReadFromJsonAsync<JsonNode>().ConfigureAwait(false);

        foreach (var item in result?.AsArray() ?? new JsonArray())
            yield return ParseRecord(item!);

        static SourceRecord ParseRecord(JsonNode node) {
            var key       = node["id"]!.GetValue<string>();
            var value     = Struct.Parser.ParseJson(node.ToJsonString());
            var eventTime = node["last_modified"]!.GetValue<DateTimeOffset>().ToUnixTimeMilliseconds();

            return new() {
                Key       = key,
                Value     = value,
                EventTime = eventTime
            };
        }
    }
}