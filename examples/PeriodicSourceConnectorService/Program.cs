using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson;
using Krimson.Connectors;
using Refit;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .Enrich.FromLogContext()
    .WriteTo.Logger(
        logger => logger.WriteTo.Console(
            theme: AnsiConsoleTheme.Literate, applyThemeToRedirectedOutput: true,
            outputTemplate: "[{Timestamp:mm:ss.fff} {Level:u3}] {SourceContext}{NewLine}{Message}{NewLine}{Exception}"
        )
    )
    .CreateLogger();

var host = Host
    .CreateDefaultBuilder(args)
    .UseSerilog(Log.Logger)
    .ConfigureServices(
        (ctx, services) => {
            services
                .AddKrimson("power-meters-cnx")
                .UseProtobuf()
                .AddProducer(pdr => pdr.Topic("foo.bar.baz"))
                .AddReader()
                .AddPullSourceConnector<PowerMetersSourceConnector>();
        }
    )
    .Build();

await host.RunAsync();

interface IPowerMetersClient {
    [Get("/meters/")]
    public Task<JsonObject?> GetMeters();
}

[BackOffTimeSeconds(1)]
class PowerMetersSourceConnector : PullSourceConnector {

    public PowerMetersSourceConnector() {
        CheckpointStrategy = DataSourceCheckpointStrategy.Batch;
    }
    
    int counter;

    public override async IAsyncEnumerable<SourceRecord> ParseRecords(PullSourceContext context) {
        for (var i = 1; i <= 500; i++) {
            counter += i;
      
            yield return new() {
                Key       = counter,
                Value     = Struct.Parser.ParseJson(@"{""success"": ""true""}"),
                EventTime = DateTimeOffset.UtcNow.AddMinutes(60).ToUnixTimeMilliseconds(),
                EventType = "powerMeterChanged"
            };
        }
    }
}