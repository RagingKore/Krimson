using Krimson.Producers;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class ProducerServicesExtensions {
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        services.AddSingleton(
            serviceProvider => {
                var configuration  = serviceProvider.GetRequiredService<IConfiguration>();
                var serializer     = serviceProvider.GetRequiredService<IDynamicSerializer>();
                
                return KrimsonProducer.Builder
                    .ReadSettings(configuration)
                    .Serializer(() => serializer)
                    .With(x => build(serviceProvider, x))
                    .Create();
            }
        );

    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        AddKrimsonProducer(services, (_, builder) => build(builder));

    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, KrimsonProducer producer) =>
        services.AddSingleton(producer);
}