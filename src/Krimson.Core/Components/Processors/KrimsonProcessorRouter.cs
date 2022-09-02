namespace Krimson.Processors;

public delegate Task ProcessRecord(KrimsonProcessorContext context);

public delegate Task ProcessMessageAsync<in T>(T message, KrimsonProcessorContext context);

public delegate void ProcessMessage<in T>(T message, KrimsonProcessorContext context);

public record ProcessorRoute(string RoutingKey, ProcessRecord Processor);

public static class ProcessMessageHandlersExtensions {
    public static KrimsonProcessorModule AsModule<T>(this ProcessMessageAsync<T> handler) => KrimsonFluentProcessorModule<T>.ForHandler(handler);
    public static KrimsonProcessorModule AsModule<T>(this ProcessMessage<T> handler)      => KrimsonFluentProcessorModule<T>.ForHandler(handler);
}

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
    public bool CanRoute(KrimsonRecord record)   => CanRoute(record.Value.GetType());
}

[PublicAPI]
public class KrimsonMasterRouter {
    static readonly ILogger Log = Serilog.Log.ForContext<KrimsonMasterRouter>();
    
    List<KrimsonProcessorModule> Modules { get; } = new();
   
    public bool HasRoutes => Modules.Any(x => x.Router.HasRoutes);
    
    //TODO SS: might need to consider strategies in the future
    public Task Process(KrimsonProcessorContext context) =>
        Modules
            .Where(x => x.SubscribesTo(context.Record))
            .Select(x => x.Process(context))
            .WhenAll();
    
    public bool CanRoute(KrimsonRecord record) =>
        Modules.Any(x => x.SubscribesTo(record));

    public KrimsonMasterRouter WithModule(KrimsonProcessorModule module) {
        if (!module.Router.HasRoutes)
            throw new InvalidOperationException($"Module {module.GetType().Name} has no routes");

        Log.Information(
            "Module {Module} routes added: {Messages}", 
            module.GetType().Name, module.Router.Routes.Select(x => x.RoutingKey)
        );
        
        Modules.Add(module);
        
        return this;
    }
    
    public KrimsonMasterRouter WithModules(IEnumerable<KrimsonProcessorModule> modules) {
        foreach (var module in modules) WithModule(module);
        return this;
    }
    
    public KrimsonMasterRouter WithHandler<T>(ProcessMessageAsync<T> handler) {
        Modules.Add(KrimsonFluentProcessorModule<T>.ForHandler(handler));
        return this;
    }
    
    public KrimsonMasterRouter WithHandler<T>(ProcessMessage<T> handler) {
        Modules.Add(KrimsonFluentProcessorModule<T>.ForHandler(handler));
        return this;
    }
}