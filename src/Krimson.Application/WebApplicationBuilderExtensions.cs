using Krimson.Configuration;
using Krimson.Processors;
using Krimson.Processors.Configuration;

namespace Krimson.Application;

public static class WebApplicationBuilderExtensions {
    public static WebApplicationBuilder UseKrimson(this WebApplicationBuilder web, Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> configureProcessor) {
        web.Services.AddKrimsonProcessor(builder => configureProcessor(builder.ReadSettings(web.Configuration)));
        return web;
    }

    // public static WebApplicationBuilder UseKrimson<TModule>(this WebApplicationBuilder web, TModule module) where TModule : KrimsonProcessorModule, new() {
    //     web.Services.AddKrimsonProcessor(builder => builder.ReadSettings(web.Configuration).Module(module));
    //     return web;
    // }
    //
    // public static WebApplicationBuilder UseKrimson<TModule>(this WebApplicationBuilder web) where TModule : KrimsonProcessorModule, new() {
    //     web.Services.AddKrimsonProcessor(builder => builder.ReadSettings(web.Configuration).Module<TModule>());
    //     return web;
    // }

    // public static WebApplicationBuilder UseKrimson<TMessage>(this WebApplicationBuilder web, ProcessMessageAsync<TMessage> processMessage) {
    //     web.Services.AddKrimsonProcessor(builder => builder.ReadSettings(web.Configuration).Process(processMessage));
    //     return web;
    // }
    //
    // public static WebApplicationBuilder UseKrimson<TMessage>(this WebApplicationBuilder web, ProcessMessage<TMessage> processMessage) {
    //     web.Services.AddKrimsonProcessor(builder => builder.ReadSettings(web.Configuration).Process(processMessage));
    //     return web;
    // }
}