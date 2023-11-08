# Krimson

Just you wait... 

A helper library for .NET that not only greatly simplifies the usage of Kafka but also covers common streaming scenarios including re-balancing.

## Usage

For a step-by-step guide and code samples, you will need to wait a bit.

Take a look in the [examples](examples) directory and at the [integration tests](tests/Krimson.Tests) for further examples.

Krimson automatically configures a schema registry instance.

### Processor Examples

#### 1. Consuming a message using a processor module

Krimson will automatically scan and register all modules.

```csharp
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
    );

builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() =>
        On<DeviceTelemetry>(
            (msg, ctx) => ctx.Logger.Information(
                "Processed {@Telemetry} with id: {RecordId}",
                msg, ctx.Record.Id
            )
        );
}
```

#### 2. Consuming a message using an inline processor

```csharp
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
            .Process<DeviceTelemetry>((msg, ctx) => ctx.Logger.Information("Processed {@Telemetry} with id: {RecordId}", msg, ctx.Record.Id))
    );

builder.Build().Run();
```

#### 3. Processing a message and outputing other messages

```csharp
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
```

### Producer Examples

#### 1. Producing a message and waiting for ack from broker (using the async method)

```csharp
using Krimson;
using Krimson.Examples.Messages.Telemetry;
using Krimson.Producers;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services
    .AddKrimson()
    .UseProtobuf()
    .AddProducer(pdx => pdx
        .ClientId("telemetry-gateway")
        .Topic("telemetry")
    );

var app = builder.Build();

if (app.Environment.IsDevelopment()) {
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();

[ApiController, Route("[controller]")]
public class IngressController : ControllerBase {
    public IngressController(KrimsonProducer producer) => Producer = producer;

    KrimsonProducer Producer { get; }
    
    [HttpPost(Name = "DispatchTelemetry")]
    public Task Dispatch(DeviceTelemetry telemetry) =>
        Producer.Produce(message: telemetry, key: telemetry.DeviceId);
}
```

### Connectors Examples

#### 1. Consuming a message using a processor module

Krimson will automatically scan and register all modules.

```csharp
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
    );

builder.Build().Run();

class TelemetryModule : KrimsonProcessorModule {
    public TelemetryModule() =>
        On<DeviceTelemetry>(
            (msg, ctx) => ctx.Logger.Information(
                "Processed {@Telemetry} with id: {RecordId}",
                msg, ctx.Record.Id
            )
        );
}
```

## Built With

* [Jetbrains Rider](https://www.jetbrains.com/rider/)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

* **Sérgio Silveira** - *Initial work* - [RagingKore](https://github.com/ragingkore)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.