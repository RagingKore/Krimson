using Microsoft.Extensions.Hosting;

namespace Krimson.Persistence.Outbox;

/// <summary>
///     A background service that processes the outbox.
/// </summary>
public class OutboxDeliveryService<T> : BackgroundService where T : IOutboxProcessor {
    public OutboxDeliveryService(T processor, TimeSpan backoffTime, OutboxProcessingStrategy strategy = OutboxProcessingStrategy.Update) {
        Processor   = processor;
        BackoffTime = backoffTime;
        Strategy    = strategy;
    }

    T                        Processor   { get; }
    TimeSpan                 BackoffTime { get; }
    OutboxProcessingStrategy Strategy    { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        while (!cancellator.IsCancellationRequested) {
            await Processor.ProcessOutbox(Strategy, cancellator).ToListAsync(cancellator.Token);
            await Tasks.SafeDelay(BackoffTime, cancellator.Token);
        }
    }
}