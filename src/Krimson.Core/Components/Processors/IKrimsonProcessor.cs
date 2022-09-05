namespace Krimson.Processors;

public interface IKrimsonProcessorInfo {
    string                            ClientId         { get; }
    string                            GroupId          { get; }
    string[]                          Topics           { get; }
    KrimsonProcessorStatus            Status           { get; }
    string                            BootstrapServers { get; }
    (string ModuleName, string Key)[] RoutingKeys      { get; }
}

public interface IKrimsonProcessor : IKrimsonProcessorInfo, IAsyncDisposable {
    Task                                            Activate(CancellationToken terminationToken, OnProcessorTerminated? onTerminated = null);
    Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGap();
    Task                                            Terminate();
}