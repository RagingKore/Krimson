using Confluent.SchemaRegistry;
using Krimson;
using Krimson.Extensions.DependencyInjection;
using Krimson.Serializers.ConfluentProtobuf;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// you can register a producer in many ways...
builder.Services.AddKrimson()
    .AddProtobuf()
    .AddProducer(pdr => pdr
        .ClientId("telemetry-gateway")
        .Topic("telemetry")
    );

builder.UseKrimson(
    krs => {
        krs.AddProducer(
            pdr => pdr
                .ClientId("telemetry-gateway")
                .Topic("telemetry")
        );
    }
);

builder.UseKrimson()
    .AddProducer(
        pdr => pdr
            .ClientId("telemetry-gateway")
            .Topic("telemetry")
    );



builder.Services.AddKrimson()
    .AddProducer(
        (provider, pdr) => pdr
            .ClientId("telemetry-gateway")
            .Topic("telemetry")
            .UseProtobuf(provider.GetRequiredService<ISchemaRegistryClient>())
    );

builder.Services.AddKrimson(
    krs => krs.AddProducer(
        (provider, pdr) => pdr
            .ClientId("telemetry-gateway")
            .Topic("telemetry")
            .UseProtobuf(provider.GetRequiredService<ISchemaRegistryClient>())
    )
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