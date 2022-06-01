using Krimson.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Configuration;

[PublicAPI]
public static class ServiceCollectionProducerExtensions {
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        services.AddSingleton(ctx => KrimsonProducer.Builder.ReadSettings(ctx.GetRequiredService<IConfiguration>()).With(x => build(ctx, x)).Create());

    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        AddKrimsonProducer(services, (_, builder) => build(builder));
}