using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Producers.Hosting;

[PublicAPI]
public static class HostingExtensions {
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IConfiguration, IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        services.AddSingleton(
            serviceProvider => {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();
                
                return KrimsonProducer.Builder
                    .ReadSettings(configuration)
                    .With(x => build(configuration, serviceProvider, x))
                    .Create();
            }
        );

    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IConfiguration, KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        AddKrimsonProducer(services, (configuration, _, builder) => build(configuration, builder));
    
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        AddKrimsonProducer(services, (_, _, builder) => build(builder));
}