using Krimson;
using Krimson.Examples.Messages.Telemetry;
using Krimson.Processors;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog(
    Log.Logger = new LoggerConfiguration()
        .Enrich.FromLogContext()
        .Enrich.WithThreadId()
        .ReadFrom.Configuration(builder.Configuration)
        .WriteTo.Async(
            x => x.Console(
                theme: AnsiConsoleTheme.Literate,
                outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext} {Message}{NewLine}{Exception}"
            )
        ).CreateLogger()
);

// by default reads appSettings, attempts to register the schema registry client and defaults to protobuf
builder.UseKrimson(
    krimson => krimson.AddProcessor(tasks: 2,
        proc => proc
            .GroupId("telemetry-processor")
            .InputTopic("telemetry.se")
            .InputTopic("telemetry.no")
            .InputTopic("telemetry.de")
    )
);

builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() {
        On<PowerConsumption>((msg, ctx) => ctx.Logger.Information("{@PowerConsumption}", msg));
        On<PowerCost>((msg, ctx) => ctx.Logger.Information("{@PowerCost}", msg));
    }
}