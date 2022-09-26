using Krimson;
using Krimson.Examples.Messages.Telemetry;
using Krimson.Processors;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKrimson()
    .UseProtobuf()
    .AddProcessor(
        prx => prx
            .ClientId("telemetry-processor")
            .InputTopic("telemetry")
            .OutputTopic("telemetry.power-consumption")
    );

builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() =>
        On<DeviceTelemetry>(
            (telemetry, ctx) => {
                if (telemetry.DataType == nameof(PowerConsumption)) {
                    var message = new PowerConsumption {
                        DeviceId  = telemetry.DeviceId,
                        Unit      = telemetry.Data.Fields["unit"].StringValue,
                        Value     = telemetry.Data.Fields["value"].NumberValue,
                        Timestamp = telemetry.Timestamp
                    };

                    ctx.Output(message: message, key: telemetry.DeviceId);
                }
                else {
                    ctx.Output(
                        message: telemetry,
                        key:     telemetry.DeviceId,
                        topic:   "telemetry.unknown"
                    );
                }
            }
        );
}

//
//
// using Krimson;
// using Krimson.Examples.Messages.Telemetry;
// using Krimson.Processors;
//
// var builder = WebApplication.CreateBuilder(args);
//
// builder.Services.AddKrimson()
//     .AddProtobuf()
//     .AddProcessor(
//         prx => prx
//             .ClientId("telemetry-processor")
//             .InputTopic("telemetry")
//             .Process<DeviceTelemetry>((msg, ctx) => ctx.Logger.Information("Processed {@Telemetry} with id: {RecordId}", msg, ctx.Record.Id))
//     );
//
// builder.Services.AddKrimson()
//     .AddProtobuf()
//     .AddProcessor(
//         prx => prx
//             .ClientId("telemetry-processor")
//             .InputTopic("telemetry")
//             .Module(new TelemetryModule())
//     );
//
// builder.Build().Run();
//
// class TelemetryModule : KrimsonProcessorModule {
//     public TelemetryModule() =>
//         On<DeviceTelemetry>(
//             (msg, ctx) => ctx.Logger.Information(
//                 "Processed {@Telemetry} with id: {RecordId}",
//                 msg, ctx.Record.Id
//             )
//         );
// }
//
// builder.Services.AddKrimson()
//     .AddProtobuf()
//     .AddProcessor(
//         prx => prx
//             .ClientId("telemetry-processor")
//             .InputTopic("telemetry")
//             .Process<DeviceTelemetry>(
//                 (msg, ctx) => ctx.Logger.Information("Processed message {@Telemetry} with id: {RecordId}", msg, ctx.Record.Id)
//             )
//     );
//
// builder.Build().Run();

//
// builder.Services.AddKrimson(
//     kri => kri
//         .AddProtobuf()
//         .AddProcessor(
//             prx => prx
//                 .ClientId("telemetry-processor")
//                 .InputTopic("telemetry")
//                 .Process<TelemetryMessage>(
//                     (msg, ctx) => ctx.Logger.Information("Processed message {@Telemetry} with id: {RecordId}", msg, ctx.Record.Id)
//                 )
//         )
// );


// using Krimson;
// using Krimson.Examples.Messages.Telemetry;
// using Krimson.Processors;
// using Serilog;
// using Serilog.Sinks.SystemConsole.Themes;
//
// var builder = WebApplication.CreateBuilder(args);
//
// builder.Host.UseSerilog(
//     Log.Logger = new LoggerConfiguration()
//         .Enrich.FromLogContext()
//         .Enrich.WithThreadId()
//         .ReadFrom.Configuration(builder.Configuration)
//         .WriteTo.Async(
//             x => x.Console(
//                 theme: AnsiConsoleTheme.Literate,
//                 outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext} {Message}{NewLine}{Exception}"
//             )
//         ).CreateLogger()
// );
//
// // by default reads appSettings, attempts to register the schema registry client and defaults to protobuf
// builder.Services.AddKrimson(
//     krimson => krimson.AddProcessor(tasks: 2,
//         proc => proc
//             .GroupId("telemetry-processor")
//             .InputTopic("telemetry.se")
//             .InputTopic("telemetry.no")
//             .InputTopic("telemetry.de")
//     )
// );
//
// builder.Build().Run();
//
// class TelemetryModule : KrimsonProcessorModule {
//     public TelemetryModule() {
//         On<PowerConsumption>((msg, ctx) => ctx.Logger.Information("{@PowerConsumption}", msg));
//         On<PowerCost>((msg, ctx) => ctx.Logger.Information("{@PowerCost}", msg));
//     }
// }