using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson;
using Krimson.Connectors;

var builder = WebApplication.CreateBuilder(args);

builder.UseKrimson()
    .AddProducer(
        pdr => pdr
            .ClientId("my_app_name")
            .Topic("foo.bar.baz")
    )
    .AddWebhooks(); // or simply call .AddWebhooks() to automatically scan and register all webhooks

var app = builder.Build();

app.UseKrimsonWebhooks();

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

    public override IAsyncEnumerable<SourceRecord> SourceRecords(IAsyncEnumerable<JsonNode> data, CancellationToken cancellationToken) {
        return data.Select(ParseSourceRecord!);

        static SourceRecord ParseSourceRecord(JsonNode node) {
            try {
                var recordId  = node["id"]!.GetValue<string>();
                var timestamp = Timestamp.FromDateTimeOffset(node["last_modified"]!.GetValue<DateTimeOffset>());
                var data      = Struct.Parser.ParseJson(node.ToJsonString());

                return new SourceRecord {
                    Id        = recordId,
                    Data      = data,
                    Timestamp = timestamp,
                    Type      = "power-meters",
                    Operation = SourceOperation.Snapshot
                };
            }
            catch (Exception) {
                return SourceRecord.Empty;
            }
        }
    }
}