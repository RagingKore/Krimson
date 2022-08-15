using Microsoft.Extensions.DependencyInjection.Extensions;
using Scrutor;

namespace Krimson.Connectors;

[PublicAPI]
public static class ServicesExtensions {
    
    public static IServiceCollection AddKrimsonPullSourceConnectors(this IServiceCollection services) {
        services.AddKrimsonReader();
        
        services.TryAddSingleton<DataSourceConsumer>();
        
        services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<PullSourceConnector>()
                        .NotInNamespaceOf<PullSourceConnector>()
                )
                .UsingRegistrationStrategy(RegistrationStrategy.Skip)
                .AsSelfWithInterfaces()
                .WithSingletonLifetime()
        );

        return services;
    }
    
    public static IServiceCollection AddKrimsonPullSourceConnector<T>(this IServiceCollection services, TimeSpan? backoffTime = null) where T : PullSourceConnector {
        services.AddKrimsonReader();
        
        services.TryAddSingleton<DataSourceConsumer>();

        services.Scan(scan => scan
            .FromAssemblyOf<T>()
            .AddClasses(classes => classes.AssignableTo<PullSourceConnector>())
            .UsingRegistrationStrategy(RegistrationStrategy.Skip)
            .AsSelfWithInterfaces()
            .WithSingletonLifetime()
        );

        return services;
    }
}