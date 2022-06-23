using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Krimson.Processors.Hosting; 

/// <summary>
/// Generic service host with clear separation between
/// task initialization and execution.
/// <para/>
/// It will also only execute after the application host
/// has fully started and initialization is complete.
/// </summary>
abstract class KrimsonBackgroundService : IHostedService, IAsyncDisposable {
    protected KrimsonBackgroundService(IHostApplicationLifetime applicationLifetime, ILogger logger) {
        ApplicationLifetime = applicationLifetime;
        Logger              = logger;
        Cancellator         = new();
        Gatekeeper          = new(false);

        ApplicationLifetime.ApplicationStarted.Register(() => Gatekeeper.Set());
    }

    protected IHostApplicationLifetime ApplicationLifetime { get; }

    ILogger                 Logger      { get; }
    CancellationTokenSource Cancellator { get; }
    ManualResetEventSlim    Gatekeeper  { get; }

    Task? ExecutingTask { get; set; }

    /// <summary>
    /// Triggered when the application host has fully started.
    /// </summary>
    /// <param name="cancellationToken">Indicates that the start process has been aborted.</param>
    public async Task StartAsync(CancellationToken cancellationToken) {

        // execute the real initialization routine
        try {
            Logger.LogTrace("initializing...");
            await Initialize(cancellationToken);
            Logger.LogDebug("initialization complete");
        }
        catch (OperationCanceledException) {
            Logger.LogWarning("initialization cancelled");
            throw;
        }
        catch (Exception ex) {
            Logger.LogCritical(ex, "initialization failed!");
            throw;
        }
        
        Logger.LogDebug("delaying execution until application host is ready...");

        _ = Task.Run(
            async () => {
                Gatekeeper.Wait(cancellationToken); 
                Gatekeeper.Dispose();

                Logger.LogTrace("application host ready, executing...");

                // Store the task we're executing
                ExecutingTask = Start(Cancellator.Token);

                try {
                    await ExecutingTask;
                }
                catch (Exception ex) {
                    Logger.LogCritical(ex, "failed to execute!");
                    throw;
                }
            }, cancellationToken
        );
    }
    
    public async ValueTask DisposeAsync() {
        Logger.LogTrace("disposing...");

        try {
            await Dispose()
                .ConfigureAwait(false);
            
            Cancellator.Dispose();
            //Gatekeeper.Dispose();
            
            Logger.LogDebug("disposed");
        }
        catch (Exception vex) {
            Logger.LogWarning(vex, "disposed violently!");
        }
    }

    
    /// <summary>
    /// Triggered when the application host is performing a graceful shutdown.
    /// </summary>
    /// <param name="cancellationToken">Indicates that the shutdown process should no longer be graceful.</param>
    public async Task StopAsync(CancellationToken cancellationToken) {
        Logger.LogTrace("stopping...");

        try {
            // Stop called without start
            if (ExecutingTask is null) {
                Logger.LogDebug("stopped awkwardly since it didn't even start");
                return;
            }

            // Links stop token to executing task token
            cancellationToken.Register(() => Cancellator.Cancel(), false);

            try {
                // Signal cancellation to the executing task
                Cancellator.Cancel();
            }
            catch (Exception ex) {
                Logger.LogDebug(ex, "failed to request task cancellation!");
            }

            // Wait until the task completes or the stop token triggers
            await Task.WhenAny(ExecutingTask, Task.Delay(Timeout.Infinite, cancellationToken));

            await Stop(cancellationToken);

            Logger.LogInformation("stopped");
        }
        catch (OperationCanceledException) {
            Logger.LogInformation("stopped suddenly on cancellation request");
            throw;
        }
        catch (Exception vex) {
            Logger.LogWarning(vex, "stopped violently!");
            throw;
        }
    }

    protected virtual Task Initialize(CancellationToken stoppingToken) => Task.CompletedTask;

    protected abstract Task Start(CancellationToken stoppingToken);

    protected abstract Task Stop(CancellationToken stoppingToken);

    protected abstract ValueTask Dispose();
}