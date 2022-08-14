using Krimson.Readers;
using Krimson.Readers.Configuration;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public static class ReaderServiceCollectionExtensions {
    public static IServiceCollection AddKrimsonReader(this IServiceCollection services, Func<IServiceProvider, KrimsonReaderBuilder, KrimsonReaderBuilder>? build = null) =>
        services.AddSingleton(
            serviceProvider => {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();
                var deserializer  = serviceProvider.GetRequiredService<IDynamicDeserializer>();
                
                var builder = KrimsonReader.Builder
                    .ReadSettings(configuration)
                    .Deserializer(() => deserializer);

                build?.Invoke(serviceProvider, builder);
                
                return builder.Create();
            }
        );
    
    public static IServiceCollection AddKrimsonReader(this IServiceCollection services, Func<KrimsonReaderBuilder, KrimsonReaderBuilder> build) =>
        AddKrimsonReader(services, (_, builder) => build(builder));
}