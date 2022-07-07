using Krimson.Examples.Messages.Telemetry;
using Krimson.Extensions.DependencyInjection;
using Krimson.Processors;
using Krimson.SchemaRegistry.Protobuf;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKrimson()
    .SchemaRegistry()
    .Processor(
        (configuration, serviceProvider, processor) => processor
            .GroupId("telemetry-processor")
            .InputTopic("telemetry")
            .UseProtobuf()
            .Module<TelemetryModule>()
    );


builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() {
        On<PowerConsumption>((msg, ctx) => ctx.Logger.Information("{@PowerConsumption}", msg));
    }
}