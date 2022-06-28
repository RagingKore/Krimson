using Krimson.Examples.Messages.Telemetry;
using Krimson.Processors;
using Krimson.Processors.Hosting;
using Krimson.SchemaRegistry.Protobuf;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKrimsonProcessor(
    tasks: 6,
    (serviceProvider, krimson) => krimson
        .Connection("localhost:9092")
        .SchemaRegistry("localhost:8081")
        .GroupId("telemetry-processor")
        .InputTopic("telemetry")
        .UseProtobuf()
        .Module<TelemetryModule>()
   
);
    
builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() {
        On<PowerConsumption>((msg, ctx) => ctx.Logger.LogInformation("{@PowerConsumption}", msg));
    }
}