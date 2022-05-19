namespace Krimson.Interceptors;

[PublicAPI]
public sealed class InterceptorCollection : ICollection<InterceptorModule> {
    static readonly ILogger Log = Serilog.Log.ForContext<InterceptorCollection>();

    public InterceptorCollection() { }
    
    public InterceptorCollection(IEnumerable<InterceptorModule> interceptors) {
        foreach (var interceptor in interceptors)
            Add(interceptor);
    }

    HashSet<InterceptorModule> RegisteredInterceptors { get; } = new();

    public async Task Intercept<T>(T evt, CancellationToken cancellationToken) where T : InterceptorEvent {
        foreach (var interceptor in RegisteredInterceptors)
            try {
                await interceptor
                    .Intercept(evt, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Warning(
                    ex,
                    "{Interceptor} failed to intercept {EventName}", 
                    interceptor.GetType().Name, typeof(T).Name
                );
            }
    }
    
    public void Intercept<T>(T evt) where T : InterceptorEvent => Intercept(evt, CancellationToken.None).Synchronously();  

    public IEnumerator<InterceptorModule> GetEnumerator() => RegisteredInterceptors.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Add(InterceptorModule interceptor) {
        if (!RegisteredInterceptors.Add(interceptor))
            throw new ArgumentException($"{interceptor.GetType().Name} already added");
    }
    
    public void Clear() => RegisteredInterceptors.Clear();

    public bool Contains(InterceptorModule interceptor)           => RegisteredInterceptors.Contains(interceptor);
    public void CopyTo(InterceptorModule[] array, int arrayIndex) => RegisteredInterceptors.CopyTo(array, arrayIndex);
    public bool Remove(InterceptorModule interceptor)             => RegisteredInterceptors.Remove(interceptor);

    public int  Count      => RegisteredInterceptors.Count;
    public bool IsReadOnly => false;
}