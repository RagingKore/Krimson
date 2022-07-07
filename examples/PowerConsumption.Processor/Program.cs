using Krimson.Examples.Messages.Telemetry;
using Krimson.Extensions.DependencyInjection;
using Krimson.Processors;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKrimson()
    .UseProtobuf()
    .Processor(
        (provider, proc) => proc
            .GroupId("telemetry-processor")
            .InputTopic("telemetry")
            .Module<TelemetryModule>()
    );

builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() {
        On<PowerConsumption>((msg, ctx) => ctx.Logger.Information("{@PowerConsumption}", msg));
    }
}