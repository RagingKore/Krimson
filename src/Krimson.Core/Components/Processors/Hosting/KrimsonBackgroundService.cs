using Microsoft.Extensions.Hosting;

namespace Krimson.Processors.Hosting; 

/// <summary>
/// Generic service host with clear separation between
/// task initialization and execution.
/// <para/>
/// It will also only execute after the application host
/// has fully started and initialization is complete.
/// </summary>
abstract class KrimsonBackgroundService : IHostedService, IAsyncDisposable {
    static readonly ILogger Logger = Log.ForContext("SourceContext", "KrimsonProcessorService");
    
    protected KrimsonBackgroundService(IHostApplicationLifetime applicationLifetime, string clientId) {
        ApplicationLifetime = applicationLifetime;
        ClientId              = clientId;
        Cancellator         = new();
        Gatekeeper          = new(false);

        ApplicationLifetime.ApplicationStarted.Register(() => Gatekeeper.Set());
    }

    protected IHostApplicationLifetime ApplicationLifetime { get; }

    string                  ClientId    { get; }
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
            Logger.Verbose("{ProcessorName} Initializing...", ClientId);
            await Initialize(cancellationToken).ConfigureAwait(false);
            Logger.Debug("{ProcessorName} Initialization complete", ClientId);
        }
        catch (OperationCanceledException) {
            Logger.Warning("{ProcessorName} Initialization cancelled", ClientId);
            throw;
        }
        catch (Exception ex) {
            Logger.Fatal(ex, "{ProcessorName} Initialization failed!", ClientId);
            throw;
        }
        
        Logger.Debug("{ProcessorName} Delaying execution until application host is ready...", ClientId);

        _ = Task.Run(
            async () => {
                Gatekeeper.Wait(cancellationToken); 
                Gatekeeper.Dispose();

                Logger.Debug("{ProcessorName} Application host ready, executing...", ClientId);

                // Store the task we're executing
                ExecutingTask = Start(Cancellator.Token);

                try {
                    await ExecutingTask.ConfigureAwait(false);
                }
                catch (Exception ex) {
                    Logger.Fatal(ex, "{ProcessorName} Failed to execute!", ClientId);
                    throw;
                }
            }, cancellationToken
        );
    }
    
    public async ValueTask DisposeAsync() {
        Logger.Verbose("{ProcessorName} Disposing...", ClientId);

        try {
            await Dispose().ConfigureAwait(false);
            
            Cancellator.Dispose();
            //Gatekeeper.Dispose();
            
            Logger.Debug("{ProcessorName} Disposed", ClientId);
        }
        catch (Exception vex) {
            Logger.Warning(vex, "{ProcessorName} Disposed violently!", ClientId);
        }
    }

    
    /// <summary>
    /// Triggered when the application host is performing a graceful shutdown.
    /// </summary>
    /// <param name="cancellationToken">Indicates that the shutdown process should no longer be graceful.</param>
    public async Task StopAsync(CancellationToken cancellationToken) {
        Logger.Verbose("{ProcessorName} Stopping...", ClientId);

        try {
            // Stop called without start
            if (ExecutingTask is null) {
                Logger.Debug("{ProcessorName} Stopped awkwardly since it didn't even start", ClientId);
                return;
            }

            // Links stop token to executing task token
            cancellationToken.Register(() => Cancellator.Cancel(), false);

            try {
                // Signal cancellation to the executing task
                Cancellator.Cancel();
            }
            catch (Exception ex) {
                Logger.Debug(ex, "{ProcessorName} Failed to request task cancellation!", ClientId);
            }

            // Wait until the task completes or the stop token triggers
            await Task
                .WhenAny(ExecutingTask, Task.Delay(Timeout.Infinite, cancellationToken))
                .ConfigureAwait(false);

            await Stop(cancellationToken).ConfigureAwait(false);

            Logger.Information("{ProcessorName} Stopped", ClientId);
        }
        catch (OperationCanceledException) {
            Logger.Information("{ProcessorName} Stopped suddenly on cancellation request", ClientId);
            throw;
        }
        catch (Exception vex) {
            Logger.Warning(vex, "{ProcessorName} Stopped violently!", ClientId);
            throw;
        }
    }

    protected virtual Task Initialize(CancellationToken stoppingToken) => Task.CompletedTask;

    protected abstract Task Start(CancellationToken stoppingToken);

    protected abstract Task Stop(CancellationToken stoppingToken);

    protected abstract ValueTask Dispose();
}