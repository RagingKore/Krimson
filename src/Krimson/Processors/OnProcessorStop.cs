namespace Krimson.Processors
{
    public delegate Task OnProcessorStop(IReadOnlyCollection<SubscriptionTopicGap> gap, Exception? exception);
}