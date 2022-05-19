namespace Krimson.Processors; 

[PublicAPI]
public static class KrimsonProcessorExtensions {
    public static async Task<IReadOnlyCollection<SubscriptionTopicGap>> RunUntilCompletion(
        this KrimsonProcessor processor, CancellationToken stoppingToken
    ) {
        var tcs = new TaskCompletionSource<IReadOnlyCollection<SubscriptionTopicGap>>();

        await processor.Start(
            stoppingToken, (gaps, ex) => {
                if (ex is null)
                    tcs.SetResult(gaps);
                else
                    tcs.SetException(ex);

                return Task.CompletedTask;
            }
        );

        return await tcs.Task;
    }

    // public static async Task RunUntilCompletion(this KafkaProcessor processor, CancellationToken stoppingToken) {
    //     var tcs = new TaskCompletionSource<bool>();
    //
    //     await processor.Start(
    //         stoppingToken, (gaps, ex) => {
    //             if (ex is null)
    //                 tcs.SetResult(true);
    //             else
    //                 tcs.SetException(ex);
    //
    //             return Task.CompletedTask;
    //         }
    //     );
    //
    //     await tcs.Task;
    // }

    public static async Task RunUntilCompletion(this KrimsonProcessor processor, OnProcessorStop? onStop, CancellationToken stoppingToken) {
        var tcs = new TaskCompletionSource<bool>();

        await processor.Start(
            stoppingToken, async (gaps, ex) => {
                try {
                    if (onStop is not null)
                        await onStop(gaps, ex);
                }
                catch (Exception) {
                    // ignored
                }

                tcs.SetResult(true);
            }
        );

        await tcs.Task;
    }

    public static Task RunUntilCompletion(this KrimsonProcessor processor, Action<Exception?>? onStop, CancellationToken stoppingToken) =>
        RunUntilCompletion(
            processor,
            (gaps, ex) => {
                onStop?.Invoke(ex);
                return Task.CompletedTask;
            },
            stoppingToken
        );
}