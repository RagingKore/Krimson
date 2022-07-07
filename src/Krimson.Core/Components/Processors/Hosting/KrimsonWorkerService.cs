// using Microsoft.Extensions.DependencyInjection;
// using Microsoft.Extensions.Hosting;
// using Serilog;
// using static Serilog.Core.Constants;
//
// namespace Krimson.Processors.Hosting; 
//
// sealed class KrimsonWorkerService : KrimsonBackgroundService {
//     public KrimsonWorkerService(
//         IKrimsonProcessor processor,
//         IServiceProvider serviceProvider,
//         Func<CancellationToken, Task>? initializeService = null
//     ) : base(
//         serviceProvider.GetRequiredService<IHostApplicationLifetime>(), 
//         Log.ForContext(SourceContextPropertyName, processor.ClientId)
//     ) {
//         InitializeService = initializeService ?? (_ => Task.CompletedTask);
//         StartService      = processor.Activate;
//         StopService       = processor.Terminate;
//         DisposeService    = processor.DisposeAsync;
//     }
//
//     Func<CancellationToken, Task>                        InitializeService { get; }
//     Func<CancellationToken, OnProcessorTerminated, Task> StartService      { get; }
//     Func<Task>                                           StopService       { get; }
//     Func<ValueTask>                                      DisposeService    { get; }
//
//     protected override Task Initialize(CancellationToken stoppingToken) => InitializeService(stoppingToken);
//
//     protected override Task Start(CancellationToken stoppingToken) =>
//         StartService(
//             stoppingToken, (_, _, ex) => {
//                 if (ex is not null)
//                     ApplicationLifetime.StopApplication();
//
//                 return Task.CompletedTask;
//             }
//         );
//
//     protected override Task Stop(CancellationToken stoppingToken) => StopService();
//
//     protected override ValueTask Dispose() => DisposeService();
// }