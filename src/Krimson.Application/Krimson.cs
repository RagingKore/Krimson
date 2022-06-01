using Krimson.Configuration;
using Krimson.Processors;
using Krimson.Processors.Configuration;

namespace Krimson.Application;

public delegate void BuildWebApplication(WebApplicationBuilder builder);

public delegate void ConfigureWebApplication(WebApplication application);

public delegate KrimsonProcessorBuilder BuildKrimsonProcessor(IServiceProvider serviceProvider, KrimsonProcessorBuilder builder);

[PublicAPI]
public sealed class Krimson {
    // public static void Run(string[] args, BuildWebApplication buildApp, ConfigureWebApplication configureApp, BuildKrimsonProcessor buildProcessor) {
    //     WebApplication.CreateBuilder(args)
    //         .With(builder => builder.Services.AddKrimsonProcessor(proc => buildProcessor(proc.ReadSettings(builder.Configuration))))
    //         .With(builder => buildApp(builder)).Build()
    //         .With(app => configureApp(app)).Run();
    // }

    // public static Task RunAsync(string[] args, BuildWebApplication buildApp, ConfigureWebApplication configureApp, BuildKrimsonProcessor buildProcessor) {
    //     return WebApplication.CreateBuilder(args)
    //         .With(builder => builder.Services.AddKrimsonProcessor(proc => buildProcessor(proc.ReadSettings(builder.Configuration))))
    //         .With(builder => buildApp(builder)).Build()
    //         .With(app => configureApp(app)).RunAsync();
    // }

    public static void Run(Action<IConfiguration, IHostEnvironment, IServiceCollection> configureServices, BuildKrimsonProcessor buildProcessor) {
        WebApplication.CreateBuilder()
            .With(web => web.Services.AddKrimsonProcessor((provider, builder) => buildProcessor(provider, builder.ReadSettings(web.Configuration))))
            .With(web => configureServices(web.Configuration, web.Environment, web.Services)).Build()
            .Run();
    }

    public static void Run<TModule>(Action<IConfiguration, IHostEnvironment, IServiceCollection> configureServices, Func<IServiceProvider, TModule> getModule)
        where TModule : KrimsonProcessorModule {
        Run(configureServices, (provider, builder) => builder.Module(getModule(provider)));
    }

    public static void Run<TModule>(Action<IConfiguration, IHostEnvironment, IServiceCollection> configureServices)
        where TModule : KrimsonProcessorModule {
        Run(configureServices, (provider, builder) => builder.Module(provider.GetRequiredService<TModule>()));
    }

    public static void Process<TMessage>(
        Action<IConfiguration, IHostEnvironment, IServiceCollection> configureServices,
        Func<IServiceProvider, ProcessMessageAsync<TMessage>> getMessageProcessor
    ) {
        Run(configureServices, (provider, builder) => {
            var messageProcessor = getMessageProcessor(provider);
            return builder.Process(messageProcessor);
        });
    }
}