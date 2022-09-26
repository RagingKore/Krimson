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