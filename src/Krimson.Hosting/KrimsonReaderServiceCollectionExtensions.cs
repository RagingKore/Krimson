using Krimson.Readers;
using Krimson.Readers.Configuration;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class KrimsonReaderServiceCollectionExtensions {
    public static IServiceCollection AddKrimsonReader(this IServiceCollection services, Func<IConfiguration, IServiceProvider, KrimsonReaderBuilder, KrimsonReaderBuilder> build) =>
        services.AddSingleton(
            serviceProvider => {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();
                var deserializer  = serviceProvider.GetRequiredService<IDynamicDeserializer>();
                
                var builder = KrimsonReader.Builder
                    .ReadSettings(configuration)
                    .Deserializer(() => deserializer)
                    .With(x => build(configuration, serviceProvider, x));

                return builder.Create();
            }
        );

    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IConfiguration, KrimsonReaderBuilder, KrimsonReaderBuilder> build) =>
        AddKrimsonReader(services, (configuration, _, builder) => build(configuration, builder));
    
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<KrimsonReaderBuilder, KrimsonReaderBuilder> build) =>
        AddKrimsonReader(services, (_, _, builder) => build(builder));
    
    public static IServiceCollection AddKrimsonProducer(this IServiceCollection services, Func<IServiceProvider, KrimsonReaderBuilder, KrimsonReaderBuilder> build) =>
        AddKrimsonReader(services, (_, serviceProvider, builder) => build(serviceProvider, builder));
}