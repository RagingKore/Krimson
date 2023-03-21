namespace Krimson.Processors;

[PublicAPI]
public abstract class KrimsonProcessorModule {
    internal KrimsonProcessorRouter Router { get; } = new KrimsonProcessorRouter();

    protected void On<T>(ProcessMessageAsync<T> handler) => Router.Register(handler);
    protected void On<T>(ProcessMessage<T> handler)      => Router.Register(handler);

    public Task Process(KrimsonProcessorContext context) => Router.Process(context);

    public bool SubscribesTo(KrimsonRecord record) => Router.CanRoute(record.Value.GetType());
}

[PublicAPI]
public sealed class KrimsonFluentProcessorModule<T> : KrimsonProcessorModule {
    public KrimsonFluentProcessorModule(ProcessMessageAsync<T> handler) => On(handler);
    public KrimsonFluentProcessorModule(ProcessMessage<T> handler) => On(handler);

    public static KrimsonProcessorModule ForHandler(ProcessMessageAsync<T> handler) => new KrimsonFluentProcessorModule<T>(handler);
    public static KrimsonProcessorModule ForHandler(ProcessMessage<T> handler)      => new KrimsonFluentProcessorModule<T>(handler);
}


public static class KrimsonProcessorModuleExtensions {

    public static Task Process(this KrimsonProcessorModule module, KrimsonRecord record, CancellationToken cancellationToken = default) =>
        module.Process(new KrimsonProcessorContext(record, cancellationToken));
}