using Krimson.Processors;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace Krimson.Application; 

public sealed class KrimsonWorkerService : KrimsonBackgroundService {
    internal KrimsonWorkerService(
        IHostApplicationLifetime applicationLifetime,
        ILogger logger,
        Func<CancellationToken, Task> initializeService,
        Func<CancellationToken, OnProcessorStop, Task> startService,
        Func<Task> stopService,
        Func<ValueTask> disposeService
    ) : base(applicationLifetime, logger) {
        InitializeService = initializeService;
        StartService      = startService;
        StopService       = stopService;
        DisposeService    = disposeService;
    }

    public KrimsonWorkerService(
        IHostApplicationLifetime applicationLifetime,
        ILogger logger,
        KrimsonProcessor processor,
        Func<CancellationToken, Task>? initializeService = null
    ) : base(applicationLifetime, logger) {
        InitializeService = initializeService ?? (_ => Task.CompletedTask);
        StartService      = processor.Start;
        StopService       = processor.Stop;
        DisposeService    = processor.DisposeAsync;
    }

    Func<CancellationToken, Task>                  InitializeService { get; }
    Func<CancellationToken, OnProcessorStop, Task> StartService      { get; }
    Func<Task>                                     StopService       { get; }
    Func<ValueTask>                                DisposeService    { get; }

    protected override Task Initialize(CancellationToken stoppingToken) => InitializeService(stoppingToken);

    protected override Task Start(CancellationToken stoppingToken) =>
        StartService(
            stoppingToken, (gap, ex) => {
                if (ex is not null)
                    ApplicationLifetime.StopApplication();

                return Task.CompletedTask;
            }
        );

    protected override Task Stop(CancellationToken stoppingToken) => StopService();

    protected override ValueTask Dispose() => DisposeService();
}