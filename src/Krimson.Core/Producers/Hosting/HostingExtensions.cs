using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Krimson.Producers.Hosting;

[PublicAPI]
public static class HostingExtensions {
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IConfiguration, IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) =>
        services.AddSingleton(
            serviceProvider => {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();
                
                return KrimsonProducer.Builder
                    .ReadSettings(configuration)
                    .LoggerFactory(serviceProvider.GetRequiredService<ILoggerFactory>())
                    .With(x => build(configuration, serviceProvider, x)).Create();
            }
        );
}