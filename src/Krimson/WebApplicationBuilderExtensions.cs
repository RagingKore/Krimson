using Krimson.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class WebApplicationBuilderExtensions {
    public static WebApplicationBuilder UseKrimson(this WebApplicationBuilder web, Action<KrimsonBuilder> configure) {
        web.Services.AddKrimson(krs => configure(krs.AddProtobuf()));
        return web;
    }
    
    public static KrimsonBuilder UseKrimson(this WebApplicationBuilder web) => 
        web.Services.AddKrimson().AddProtobuf();
}