using Microsoft.Extensions.Hosting;

namespace Krimson.Persistence.Outbox;

/// <summary>
///     A background service that cleans up the outbox.
/// </summary>
public class OutboxCleanUpService<T> : BackgroundService where T : IOutboxProcessor {
    public OutboxCleanUpService(T processor, TimeSpan backoffTime, TimeSpan retentionPeriod) {
        Processor       = processor;
        BackoffTime     = backoffTime;
        RetentionPeriod = retentionPeriod;
    }

    T        Processor       { get; }
    TimeSpan BackoffTime     { get; }
    TimeSpan RetentionPeriod { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        while (!cancellator.IsCancellationRequested) {
            await Processor.CleanUpOutbox(RetentionPeriod, cancellator.Token);
            await Tasks.SafeDelay(BackoffTime, cancellator.Token);
        }
    }
}