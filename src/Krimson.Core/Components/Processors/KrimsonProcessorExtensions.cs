namespace Krimson.Processors; 

[PublicAPI]
public static class KrimsonProcessorExtensions {
    public static async Task<IReadOnlyCollection<SubscriptionTopicGap>> RunUntilCompletion(this KrimsonProcessor processor, CancellationToken stoppingToken) {
        var tcs = new TaskCompletionSource<IReadOnlyCollection<SubscriptionTopicGap>>();

        await processor.Activate(
            stoppingToken, (proc, gaps, ex) => {
                if (ex is null)
                    tcs.SetResult(gaps);
                else
                    tcs.SetException(ex);

                return Task.CompletedTask;
            }
        );

        return await tcs.Task;
    }
    
    public static async Task RunUntilCompletion(this KrimsonProcessor processor, OnProcessorTerminated? onTerminated, CancellationToken stoppingToken) {
        var tcs = new TaskCompletionSource<bool>();

        await processor.Activate(
            stoppingToken, async  (proc, gaps, ex) => {
                if (onTerminated is not null) 
                    await onTerminated(proc, gaps, ex).ConfigureAwait(false);
                
                tcs.SetResult(true);
            }
        );

        await tcs.Task;
    }

    public static Task RunUntilCompletion(this KrimsonProcessor processor, Action<IReadOnlyCollection<SubscriptionTopicGap>, Exception?>? onTerminated, CancellationToken stoppingToken) =>
        RunUntilCompletion(
            processor,
            (_, gaps, ex) => {
                onTerminated?.Invoke(gaps, ex);
                return Task.CompletedTask;
            },
            stoppingToken
        );
}