using Elaway.Toolkit.Http;
using Krimson;
using Krimson.Connectors.Http;

await HttpApplication
    .Build(
        builder => {
            builder.UseKrimson()
                .AddReader(rdr => rdr.ClientId(builder.Configuration["Application:Name"]))
                .AddProducer(
                    pdr => pdr
                        .ClientId(builder.Configuration["Application:Name"])
                        .Topic("elw.platform.device-registry.sources.volte")
                ).AddWebhookHandler<WebhookHandler>();
        }
    )
    .Configure(application => { application.AddWebhookConnector("/mywebhook"); })
    .Run();