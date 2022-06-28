namespace Krimson.Processors; 

[PublicAPI]
public static class KrimsonProcessorExtensions {
    public static async Task<IReadOnlyCollection<SubscriptionTopicGap>> RunUntilCompletion(this KrimsonProcessor processor, CancellationToken stoppingToken) {
        var tcs = new TaskCompletionSource<IReadOnlyCollection<SubscriptionTopicGap>>();

        await processor.Start(
            stoppingToken, (proc, sub, gaps, ex) => {
                if (ex is null)
                    tcs.SetResult(gaps);
                else
                    tcs.SetException(ex);

                return Task.CompletedTask;
            }
        );

        return await tcs.Task;
    }
    
    public static async Task RunUntilCompletion(this KrimsonProcessor processor, OnProcessorStop? onStop, CancellationToken stoppingToken) {
        var tcs = new TaskCompletionSource<bool>();

        await processor.Start(
            stoppingToken, async  (proc, sub, gaps, ex) => {
                if (onStop is not null) 
                    await onStop(proc, sub, gaps, ex).ConfigureAwait(false);
                
                tcs.SetResult(true);
            }
        );

        await tcs.Task;
    }

    public static Task RunUntilCompletion(this KrimsonProcessor processor, Action<IReadOnlyCollection<SubscriptionTopicGap>, Exception?>? onStop, CancellationToken stoppingToken) =>
        RunUntilCompletion(
            processor,
            (_, _, gaps, ex) => {
                onStop?.Invoke(gaps, ex);
                return Task.CompletedTask;
            },
            stoppingToken
        );
}