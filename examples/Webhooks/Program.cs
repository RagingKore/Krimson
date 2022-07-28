using Krimson;
using Krimson.Connectors.Http;
using Microsoft.AspNetCore.Builder;

var builder = WebApplication.CreateBuilder(args);

builder.UseKrimson()
    .AddReader(rdr => rdr.ClientId(builder.Configuration["Application:Name"]))
    .AddProducer(
        pdr => pdr
            .ClientId(builder.Configuration["Application:Name"])
            .Topic("foo.bar.baz")
    )
    .AddWebhookHandler<WebhookHandler>();

var app = builder.Build();

app.AddWebhookConnector("/mywebhook");

app.MapGet("/", () => "Hello World!");

app.Run();