using Krimson.Examples.Messages.Telemetry;
using Krimson.Processors;

var builder = WebApplication.CreateBuilder(args);

// // by default reads appSettings, attempts to register the schema registry client and defaults to protobuf
// builder.UseKrimson(
//     krimson => krimson.AddProcessor(
//         proc => proc
//             .GroupId("telemetry-processor")
//             .InputTopic("telemetry")
//             .Module<TelemetryModule>()
//     )
// );
//
// builder.UseKrimson()
//     .AddProcessor(
//         proc => proc
//             .GroupId("telemetry-processor")
//             .InputTopic("telemetry")
//             .Module<TelemetryModule>()
//     );

// by default reads appSettings and attempts to register the schema registry client
// builder.Services.AddKrimson()
//     .UseProtobuf()
//     .Processor(
//         (provider, proc) => proc
//             .GroupId("telemetry-processor")
//             .InputTopic("telemetry")
//             .Module<TelemetryModule>()
//     );

builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() {
        On<PowerConsumption>((msg, ctx) => ctx.Logger.Information("{@PowerConsumption}", msg));
    }
}