namespace Krimson.Processors;

public interface IKrimsonProcessorInfo {
    string                 ClientId         { get; }
    string                 GroupId          { get; }
    string[]               Topics           { get; }
    KrimsonProcessorStatus Status           { get; }
    string                 BootstrapServers { get; }
}

public interface IKrimsonProcessor : IKrimsonProcessorInfo, IAsyncDisposable {
    Task                                            Activate(CancellationToken terminationToken, OnProcessorTerminated? onTerminated = null);
    Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGap();
    Task                                            Terminate();
}

// public class KrimsonMasterProcessor : IKrimsonProcessor {
//     public KrimsonMasterProcessor(string processorName, IEnumerable<IKrimsonProcessor> processors) {
//         ProcessorName    = processorName;
//         Processors       = processors.ToList();
//         SubscriptionName = Processors.First().SubscriptionName;
//         Topics           = Processors.First().Topics;
//     }
//
//     List<IKrimsonProcessor> Processors { get; }
//
//     public string                 ProcessorName    { get; }
//     public string                 SubscriptionName { get; }
//     public string[]               Topics           { get; }
//     public KrimsonProcessorStatus Status           { get; } = GetStatus(Processors);
//
//     static KrimsonProcessorStatus GetStatus() {
//         return Processors.First().Status;
//     }
//
//     public async Task Start(CancellationToken stoppingToken, OnProcessorStop? onStop = null) {
//         foreach (var processor in Processors)
//             await processor.Start(stoppingToken, onStop).ConfigureAwait(false);
//     }
//
//     /// <summary>
//     /// Wont work cause it needs the processor name per gaps collection...
//     /// </summary>
//     public async Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGap() {
//         var gaps = new List<SubscriptionTopicGap>();
//
//         foreach (var processor in Processors) {
//             var procGaps = await processor.GetSubscriptionGap().ConfigureAwait(false);
//             gaps.AddRange(procGaps);
//         }
//
//         return gaps;
//     }
//
//     public async Task Stop() {
//         foreach (var processor in Processors)
//             await processor.Stop().ConfigureAwait(false);
//     }
//
//     public async ValueTask DisposeAsync() {
//         foreach (var processor in Processors)
//             await processor.DisposeAsync().ConfigureAwait(false);
//     }
// }