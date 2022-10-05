using Krimson.Client.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Krimson;

[PublicAPI]
public static class ClientServiceCollectionExtensions {
    public static IServiceCollection AddKrimsonClient(this IServiceCollection services, Func<IServiceProvider, KrimsonClientBuilder, KrimsonClientBuilder>? build = null) {
        services.TryAddSingleton(
            serviceProvider => {
                var configuration = serviceProvider.GetRequiredService<IConfiguration>();

                var builder = KrimsonClient.Builder.ReadSettings(configuration);

                build?.Invoke(serviceProvider, builder);

                return builder.Create();
            }
        );

        return services;
    }

    public static IServiceCollection AddKrimsonClient(this IServiceCollection services, Func<KrimsonClientBuilder, KrimsonClientBuilder> build) =>
        AddKrimsonClient(services, (_, builder) => build(builder));
}