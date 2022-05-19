namespace Krimson.Interceptors;

public delegate void Intercept(InterceptorEvent evt);

[PublicAPI]
public abstract class InterceptorModule {
    Dictionary<string, Func<object, CancellationToken, Task>> Handlers { get; } = new();

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
        var key = typeof(T).FullName!;

        if (Handlers.TryGetValue(key, out var handler))
            await handler(interceptorEvent, cancellationToken).ConfigureAwait(false);
    }
}