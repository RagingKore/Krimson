namespace Krimson.Processors;

public delegate Task ProcessRecord(KrimsonProcessorContext context);

public delegate Task ProcessMessageAsync<in T>(T message, KrimsonProcessorContext context);

public delegate void ProcessMessage<in T>(T message, KrimsonProcessorContext context);

public record ProcessorRoute(string RoutingKey, ProcessRecord Processor);

[PublicAPI]
public class KrimsonProcessorRouter {
    Dictionary<string, ProcessRecord> Handlers { get; } = new();
    
    public bool HasRoutes => Handlers.Any();

    public IEnumerable<ProcessorRoute> Routes => Handlers.Select(x => new ProcessorRoute(x.Key, x.Value));

    public KrimsonProcessorRouter Register<T>(string routingKey, ProcessMessageAsync<T> handler) {
        if (!Handlers.TryAdd(routingKey, ctx => handler((T)ctx.Record.Value, ctx)))
            throw new Exception($"Message already subscribed: {routingKey}");

        return this;
    }

    public KrimsonProcessorRouter Register<T>(string routingKey, ProcessMessage<T> handler) =>
        Register<T>(
            routingKey,
            (msg, ctx) => {
                handler(msg, ctx);
                return Task.CompletedTask;
            }
        );

    public KrimsonProcessorRouter Register<T>(ProcessMessageAsync<T> handler) => Register(typeof(T).FullName!, handler);
    public KrimsonProcessorRouter Register<T>(ProcessMessage<T> handler)      => Register(typeof(T).FullName!, handler);

    public Task Process(string routingKey, KrimsonProcessorContext context) => Handlers[routingKey](context);
    public Task Process(KrimsonProcessorContext context)                    => Process(context.Record.Value.GetType().FullName!, context);

    public bool CanRoute(string messageTypeName) => Handlers.ContainsKey(messageTypeName);
    public bool CanRoute(Type messageType)       => CanRoute(messageType.FullName!);
    public bool CanRoute(KrimsonRecord record)     => CanRoute(record.Value.GetType());
}