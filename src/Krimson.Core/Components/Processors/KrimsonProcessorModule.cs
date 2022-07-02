namespace Krimson.Processors; 

[PublicAPI]
public abstract class KrimsonProcessorModule {
    internal KrimsonProcessorRouter Router { get; } = new KrimsonProcessorRouter();

    protected void On<T>(ProcessMessageAsync<T> handler) => Router.Register(handler);
    protected void On<T>(ProcessMessage<T> handler)      => Router.Register(handler);

    public Task Process(KrimsonProcessorContext context) => Router.Process(context);
    
    public bool SubscribesTo(KrimsonRecord record)  => Router.CanRoute(record.Value.GetType());
}