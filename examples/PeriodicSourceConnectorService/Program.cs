using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson;
using Krimson.Connectors;
using Krimson.Extensions.DependencyInjection;
using Refit;
using Timestamp = Confluent.Kafka.Timestamp;

var host = Host
    .CreateDefaultBuilder(args)
    .ConfigureServices((ctx, services) => {
            services.AddKrimson()
                .AddProtobuf()
                .AddPeriodicSourceConnector<PowerMetersConnector>()
                .AddProducer(pdr => pdr.ClientId("my_app_name").Topic("foo.bar.baz"))
                .AddReader(rdr => rdr.ClientId("my_app_name"));

            services.AddRefitClient<IPowerMetersClient>()
                .ConfigureHttpClient(
                    client => {
                        client.BaseAddress                         = new(ctx.Configuration["PowerMetersClient:Url"]);
                        client.DefaultRequestHeaders.Authorization = new("Token", ctx.Configuration["PowerMetersClient:ApiKey"]);
                    }
                );
        }
    )
    .Build();

await host.RunAsync();

interface IPowerMetersClient {
    [Get("/meters/")]
    public Task<JsonObject?> GetMeters();
}

[BackOffTimeSeconds(30)]
class PowerMetersConnector : KrimsonPeriodicSourceConnector {
    public PowerMetersConnector(IPowerMetersClient client) => Client = client;

    IPowerMetersClient Client { get; }

    public override async IAsyncEnumerable<JsonNode> SourceData(KrimsonPeriodicSourceConnectorContext context) {
        var result = await Client.GetMeters().ConfigureAwait(false);

        foreach (var item in result?.AsArray() ?? new JsonArray())
            yield return item!;
    }
    
    public override SourceRecord ParseSourceRecord(JsonNode node) {
        var key       = node["id"]!.GetValue<string>();
        var timestamp = new Timestamp(node["last_modified"]!.GetValue<DateTimeOffset>());
        var data      = Struct.Parser.ParseJson(node.ToJsonString());

        return new() {
            Key       = key,
            Value     = data,
            Timestamp = timestamp,
            Headers   = new () { { "source", "power-meters" } }
        };
    }
}