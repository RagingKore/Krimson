namespace Krimson.Processors; 

public delegate Task OnProcessorTerminated(IKrimsonProcessorInfo processor, IReadOnlyCollection<SubscriptionTopicGap> gap, Exception? exception);