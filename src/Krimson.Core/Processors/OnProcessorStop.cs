namespace Krimson.Processors
{
    public delegate Task OnProcessorStop(string processor, string subscriptionName, IReadOnlyCollection<SubscriptionTopicGap> gap, Exception? exception);
}