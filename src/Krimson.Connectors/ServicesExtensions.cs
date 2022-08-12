using Krimson.Connectors.Http;
using Krimson.Producers;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Krimson.Connectors;

public static class KrimsonConnectorsServicesExtensions {
    public static IServiceCollection AddKrimsonDataSource<T>(this IServiceCollection services) where T : class, IDataSource => 
        services.AddSingleton<IDataSource, T>();

    public static IServiceCollection AddKrimsonDataSources(this IServiceCollection services) {
        return services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<IDataSource>()
                        .NotInNamespaceOf<IDataSource>()
                )
                .AsImplementedInterfaces()
                .WithSingletonLifetime()
        );
    }

    public static IServiceCollection AddKrimsonDataSourcesFromNamespaces(this IServiceCollection services, params string[] namespaces) =>
        services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<IDataSource>()
                        .NotInNamespaceOf<IDataSource>()
                        .InNamespaces(namespaces)
                )
                .AsImplementedInterfaces()
                .WithSingletonLifetime()
        );
    
    public static IServiceCollection AddKrimsonDataSourcesInNamespaceOf<T>(this IServiceCollection services) =>
        services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(
                    classes => classes
                        .AssignableTo<IDataSource>()
                        .NotInNamespaceOf<IDataSource>()
                        .InNamespaceOf<T>()
                )
                .AsImplementedInterfaces()
                .WithSingletonLifetime()
        );
}