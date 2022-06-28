using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Producers.Hosting;

[PublicAPI]
public static class HostingExtensions {
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        services.AddSingleton(ctx => KrimsonProducer.Builder.ReadSettings(ctx.GetRequiredService<IConfiguration>()).With(x => build(ctx, x)).Create());

    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        AddKrimsonProducer(services, (_, builder) => build(builder));
}