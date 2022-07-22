using Krimson.Hosting;
using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Krimson;

[PublicAPI]
public static class KrimsonProcessorServiceCollectionExtensions {
    record KrimsonProcessorRegistration(int Order, KrimsonProcessorBuilder Builder);

    static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<IConfiguration, IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        int tasks = 1,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Ensure.NotNull(build, nameof(build));
        Ensure.Positive(tasks, nameof(tasks));

        for (var i = 1; i <= tasks; i++) {
            var order = i;
            services.AddSingleton(ctx => AddWorker(ctx, order));
        }

        return services;

        IHostedService AddWorker(IServiceProvider ctx, int order) {
            var configuration  = ctx.GetRequiredService<IConfiguration>();
            var serializer     = ctx.GetRequiredService<IDynamicSerializer>();
            var deserializer   = ctx.GetRequiredService<IDynamicDeserializer>();
            var module         = ctx.GetService<KrimsonProcessorModule>();
            
            var builder = KrimsonProcessor.Builder
                .ReadSettings(configuration)
                .Serializer(() => serializer)
                .Deserializer(() => deserializer)
                .Module(() => module)
                .With(x => build(configuration, ctx, x));

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
        Func<IConfiguration, IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, build, tasks, initialize);
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<IConfiguration, IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, build, 1, initialize);
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        int tasks,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, (_, provider, builder) => build(provider, builder), tasks, initialize);
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, (_, provider, builder) => build(provider, builder), 1, initialize);
    
    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, (_, provider, builder) => build(builder), 1, initialize);
}