using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Krimson.Interceptors;

public delegate void Intercept(InterceptorEvent evt);

[PublicAPI]
public abstract class InterceptorModule {
    Dictionary<string, Func<object, CancellationToken, Task>> Handlers { get; } = new();

    protected InterceptorModule() {
        Name   = GetType().Name;
        Logger = new NullLoggerFactory().CreateLogger(Name);
    }

    public virtual string Name { get; set; }

    protected ILogger Logger { get; private set; }
    
    protected void On<T>(Func<T, CancellationToken, Task> handler) where T : InterceptorEvent {
        var key = typeof(T).FullName!;

        if (!Handlers.TryAdd(key, (evt, ct) => handler((T)evt, ct)))
            throw new($"Event already registered for interception: {key}");
    }

    protected void On<T>(Action<T> handler) where T : InterceptorEvent =>
        On<T>(
            (evt, _) => {
                handler(evt);
                return Task.CompletedTask;
            }
        );

    public async Task Intercept<T>(T interceptorEvent, CancellationToken cancellationToken) where T : InterceptorEvent {
        Ensure.NotNull(interceptorEvent, nameof(interceptorEvent));
        
        var key = interceptorEvent.GetType().FullName!;

        if (Handlers.TryGetValue(key, out var handler))
            await handler(interceptorEvent, cancellationToken).ConfigureAwait(false);
    }
    
    public void SetLoggerFactory(ILoggerFactory loggerFactory) {
        Ensure.NotNull(loggerFactory, nameof(loggerFactory));
        Logger = loggerFactory.CreateLogger(Name);
    }

    public void SetName(string name) {
        Ensure.NotNullOrWhiteSpace(name, nameof(name));
        Name = name;
    }
}