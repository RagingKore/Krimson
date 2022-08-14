namespace Krimson.Connectors;


class PushSourceWorker<TSource> : BackgroundService where TSource : AutonomousPeriodicPushSource {
    public PushSourceWorker(TSource source, IServiceProvider services) {
        Source   = source;
        Services = services;
    }

    TSource          Source   { get; }
    IServiceProvider Services { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        Source.Initialize(Services);
        
        await Source
            .Execute(new PushSourceContext(Services, stoppingToken))
            .ConfigureAwait(false);
    }
}

//
// class PeriodicSourceService<TSourceModule> : BackgroundService where TSourceModule : PeriodicSourceModule {
//     public PeriodicSourceService(TSourceModule module, PeriodicSource source) {
//         Module = module;
//         Source = source;
//     }
//
//     TSourceModule  Module { get; }
//     PeriodicSource Source { get; }
//
//     protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
//         var context = new PeriodicSourceContext(null, stoppingToken);
//
//         while (!stoppingToken.IsCancellationRequested) {
//             try {
//                 await Source
//                     .Execute(Module, context)
//                     .ConfigureAwait(false);
//
//                 await Task.Delay(Module.BackoffTime, context.CancellationToken).ConfigureAwait(false);
//             }
//             catch (OperationCanceledException) {
//                 // be kind and don't crash on cancellation
//                 //Log.Debug("stopped on user cancellation request");
//             }
//         }
//     }
// }