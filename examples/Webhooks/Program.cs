using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson;
using Krimson.Connectors;
using Krimson.Connectors.Http;
using Timestamp = Confluent.Kafka.Timestamp;

var builder = WebApplication.CreateBuilder(args);

builder.UseKrimson()
    .AddProducer(
        pdr => pdr
            .ClientId("my_app_name")
            .Topic("foo.bar.baz")
    )
    .AddWebhooks(); // or simply call .AddWebhooks() to automatically scan and register all webhooks

var app = builder.Build();

app.UseKrimsonWebhookSources();

app.MapGet("/", () => "Hello World!");

app.Run();


[WebhookPath("/meters")]
class PowerMetersWebhook : KrimsonWebhook {
    // public override string WebhookPath => "/meters"; // option 2

    // public PowerMetersWebhook() : base("/meters") { } // options 3

    public override ValueTask<bool> Validate(KrimsonWebhookContext context) {
        var header = context.Request.Headers["X-Signature"].ToString();
        return ValueTask.FromResult(header == "this_is_fine");
    }
    
    public override SourceRecord ParseSourceRecord(JsonNode node) {
        var key       = node["id"]!.GetValue<string>();
        var timestamp = new Timestamp(node["last_modified"]!.GetValue<DateTimeOffset>());
        var data      = Struct.Parser.ParseJson(node.ToJsonString());

        return new() {
            Key       = key,
            Value     = data,
            Timestamp = timestamp,
            Headers   = new() { { "source", "power-meters" } }
        };
    }
}