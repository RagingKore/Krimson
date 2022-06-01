using Krimson.Examples.Messages.Telemetry;
using Krimson.Processors;
using Krimson.Processors.Hosting;
using Krimson.SchemaRegistry.Protobuf;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKrimsonProcessor(
    tasks: 6,
    (ctx, krimson) => krimson
        .Connection("localhost:9092", "user", "pass")
        .SchemaRegistry("localhost:8081", "user", "pass")
        .GroupId("telemetry-processor")
        .InputTopic("telemetry")
        .Module<TelemetryModule>()
        .UseProtobuf()
);
    
builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() {
        On<PowerConsumption>((msg, ctx) => ctx.Logger.LogInformation("{PowerConsumption}", msg));
    }
}