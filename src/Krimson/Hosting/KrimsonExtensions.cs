using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Krimson.Hosting;

public static partial class KrimsonExtensions {
    public static IServiceCollection AddKrimson(this IServiceCollection services, Func<KrimsonConfigurator, KrimsonConfigurator> configure) {
        _ = configure(new KrimsonConfigurator {
            Services = services,
            Options  = new()
        });

        return services;
    }

    public static IServiceCollection AddKrimson(this IServiceCollection services, IConfiguration configuration, Func<KrimsonConfigurator, KrimsonConfigurator> configure) {
        _ = configure(
            new KrimsonConfigurator {
                Services = services,
                Options  = new()
            }.Configuration(configuration)
        );

        return services;
    }
}

static partial class KrimsonExtensions {
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
                    var builder = KrimsonProcessor.Builder.With(x => build(ctx, x));

                    if (tasks > 1) {
                        builder = builder
                            .SubscriptionName(builder.Options.ConsumerConfiguration.GroupId)
                            .ProcessorName($"{builder.Options.ConsumerConfiguration.ClientId}-{order + 1:000}");
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
}

static partial class KrimsonExtensions {
    public static IServiceCollection AddKrimsonProducer(
        this IServiceCollection services, Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build
    ) =>
        services.AddSingleton(ctx => KrimsonProducer.Builder.With(x => build(ctx, x)).Create());
}