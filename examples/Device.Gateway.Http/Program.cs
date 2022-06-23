using Krimson.Producers;
using Krimson.Producers.Hosting;
using Krimson.SchemaRegistry.Protobuf;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// KrimsonProducer.Builder
//     .ReadSettings(builder.Configuration)
//     .ProducerName("gateway")
//     .Topic("telemetry")
//     .UseProtobuf()
//     .Create();

builder.Services.AddKrimsonProducer(
    producer => producer
        .ReadSettings(builder.Configuration)
        .UseProtobuf()
);

builder.Services.AddKrimsonProducer(
    producer => producer
        .ClientId("telemetry-gateway")
        .Topic("telemetry")
        .UseProtobuf()
);

builder.Services.AddSingleton(
    ctx => KrimsonProducer.Builder
        .ReadSettings(ctx.GetRequiredService<IConfiguration>())
        .ClientId("gateway")
        .Topic("telemetry")
        .UseProtobuf()
        .Create()
);

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment()) {
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();