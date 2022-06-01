using Krimson.Configuration;
using Krimson.Processors;
using Krimson.Processors.Configuration;

namespace Krimson.Application;

[PublicAPI]
public static class ServiceCollectionProcessorExtensions {
    record KrimsonProcessorRegistration(int Order, KrimsonProcessorBuilder Builder);

    static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        int tasks = 1,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Ensure.NotNull(build, nameof(build));
        Ensure.Positive(tasks, nameof(tasks));

        for (var i = 0; i < tasks; i++) {
            RegisterProcessor(i);
            RegisterWorker(i);
        }

        return services;

        void RegisterProcessor(int order)
            => services.AddSingleton(
                ctx => {
                    var builder = KrimsonProcessor.Builder
                        .ReadSettings(ctx.GetRequiredService<IConfiguration>())
                        .With(x => build(ctx, x));

                    if (tasks > 1) {
                        builder = builder
                            .GroupId(builder.Options.ConsumerConfiguration.GroupId)
                            .ClientId($"{builder.Options.ConsumerConfiguration.ClientId}-{order + 1:000}");
                    }

                    return new KrimsonProcessorRegistration(order, builder);
                }
            );

        void RegisterWorker(int order)
            => services.AddSingleton<IHostedService>(
                ctx => {
                    var processor = ResolveProcessor(ctx, order);
                    var lifetime  = ctx.GetRequiredService<IHostApplicationLifetime>();

                    var logger = ctx
                        .GetRequiredService<ILoggerFactory>()
                        .CreateLogger(processor.ProcessorName);
                    
                    return new KrimsonWorkerService(
                        lifetime, logger, processor,
                        ct => initialize?.Invoke(ctx, ct) ?? Task.CompletedTask
                    );
                }
            );

        static KrimsonProcessor ResolveProcessor(IServiceProvider serviceProvider, int order) {
            return serviceProvider
                .GetServices<KrimsonProcessorRegistration>()
                .OrderBy(x => x.Order)
                .ToArray()[order]
                .Builder.Create();
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
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) => AddKrimsonProcessor(services, build, 1, initialize);

    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        int tasks,
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) =>
        AddKrimsonProcessor(services, tasks, (_, builder) => build(builder), initialize);

    public static IServiceCollection AddKrimsonProcessor(
        this IServiceCollection services,
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) =>
        AddKrimsonProcessor(services, (_, builder) => build(builder), initialize);
}