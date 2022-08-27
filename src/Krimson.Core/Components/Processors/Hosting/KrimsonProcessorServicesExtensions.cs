using Krimson.Hosting;
using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Scrutor;

namespace Krimson;

[PublicAPI]
public static class KrimsonProcessorServicesExtensions {
    record KrimsonProcessorRegistration(int Order, KrimsonProcessorBuilder Builder);

    static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        int tasks = 1,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Ensure.NotNull(build, nameof(build));
        Ensure.Positive(tasks, nameof(tasks));

        services.AddKrimsonModules();
        
        for (var i = 1; i <= tasks; i++) {
            var order = i;
            services.AddSingleton(ctx => AddWorker(ctx, order));
        }

        return services;

        IHostedService AddWorker(IServiceProvider ctx, int order) {
            var configuration  = ctx.GetRequiredService<IConfiguration>();
            var serializer     = ctx.GetRequiredService<IDynamicSerializer>();
            var deserializer   = ctx.GetRequiredService<IDynamicDeserializer>();
            var modules        = ctx.GetServices<KrimsonProcessorModule>();
            
            var builder = KrimsonProcessor.Builder
                .ReadSettings(configuration)
                .Serializer(() => serializer)
                .Deserializer(() => deserializer)
                .Modules(modules)
                .With(x => build(ctx, x));

            if (order > 1) {
                builder = builder
                    .GroupId(builder.Options.ConsumerConfiguration.GroupId)
                    .ClientId($"{builder.Options.ConsumerConfiguration.ClientId}-{order:000}");
            }

            var processor = builder.Create();
        
            return new KrimsonWorkerService(
                processor, ctx, ct => initialize?.Invoke(ctx, ct) ?? Task.CompletedTask
            );
        }
    }
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        int tasks,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, build, tasks, initialize);
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        int tasks,
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, (_, builder) => build(builder), tasks, initialize);
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, build, 1, initialize);
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, (_, builder) => build(builder), 1, initialize);

    static IServiceCollection AddKrimsonModules(this IServiceCollection services) =>
        services.Scan(
            scan => scan.FromApplicationDependencies()
                .AddClasses(classes => classes
                    .AssignableTo<KrimsonProcessorModule>()
                    .NotInNamespaceOf<KrimsonProcessorModule>()
                )
                .As<KrimsonProcessorModule>()
                .AsImplementedInterfaces()
                .AsSelf()
                .WithSingletonLifetime()
        );
}