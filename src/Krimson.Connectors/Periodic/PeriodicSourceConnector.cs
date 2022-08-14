namespace Krimson.Connectors;

public abstract class PeriodicSourceConnector : ExecutableDataSource<PeriodicSourceContext> { }

// public abstract class PeriodicSourceConnector : ExecutableDataSource<PeriodicSourceContext>, IHostedService {
//     protected PeriodicSourceConnector(IServiceProvider services, TimeSpan? backoffTime = null) {
//         Services    = services;
//         BackoffTime = backoffTime ?? GetBackoffTime(GetType());
//         
//         static TimeSpan GetBackoffTime(Type type) =>
//             (BackOffTimeAttribute?) Attribute.GetCustomAttribute(type, typeof(BackOffTimeAttribute)) ?? TimeSpan.FromSeconds(30);
//     }
//
//     IServiceProvider         Services    { get; }
//     TimeSpan                 BackoffTime { get; }
//     
//     Task?                    ExecuteTask { get; set; }
//     CancellationTokenSource? Cancellator { get; set; }
//     
//     /// <summary>
//     /// Triggered when the application host is ready to start the service.
//     /// </summary>
//     /// <param name="cancellationToken">Indicates that the start process has been aborted.</param>
//     public Task StartAsync(CancellationToken cancellationToken) {
//         Cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
//    
//         // ReSharper disable once MethodSupportsCancellation
//         ExecuteTask = Task.Run(async () => {
//             var context = new PeriodicSourceContext(Services, Cancellator.Token);
//             while (!context.CancellationToken.IsCancellationRequested) {
//                 await base.Execute(context).ConfigureAwait(false);
//                 await Task.Delay(BackoffTime, context.CancellationToken).ConfigureAwait(false);
//             }  
//         });
//
//         return ExecuteTask.IsCompleted ? ExecuteTask : Task.CompletedTask;
//     }
//
//     /// <summary>
//     /// Triggered when the application host is performing a graceful shutdown.
//     /// </summary>
//     /// <param name="cancellationToken">Indicates that the shutdown process should no longer be graceful.</param>
//     public async Task StopAsync(CancellationToken cancellationToken)
//     {
//         // Stop called without start
//         if (ExecuteTask is null)
//             return;
//
//         try {
//             // Signal cancellation to the executing method
//             Cancellator?.Cancel();
//         }
//         finally {
//             // Wait until the task completes or the stop token triggers
//             await Task.WhenAny(ExecuteTask, Task.Delay(Timeout.Infinite, cancellationToken)).ConfigureAwait(false);
//         }
//     }
//
//     public override ValueTask DisposeAsync() {
//         Cancellator?.Cancel();
//         return ValueTask.CompletedTask;
//     }
// }