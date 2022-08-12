namespace Krimson.Connectors;

// class PushSourceBackgroundService<TSourceModule> : BackgroundService where TSourceModule : PeriodicSourceModule {
//     public PushSourceBackgroundService(TSourceModule module, PeriodicSource source) {
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