using Serilog;

namespace Krimson.Interceptors;

[PublicAPI]
public record InterceptorCollection : IReadOnlyCollection<InterceptorModule>, IAsyncDisposable {
    public InterceptorCollection() {
        Interceptors = new HashSet<InterceptorModule>();
        Logger       = Log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, nameof(Interceptors));
    }

    HashSet<InterceptorModule> Interceptors  { get; init; }
    ILogger                    Logger        { get; init; }
    
    public async Task Intercept<T>(T evt, CancellationToken cancellationToken) where T : InterceptorEvent {
        foreach (var interceptor in Interceptors)
            try {
                await interceptor
                    .Intercept(evt, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) {
                Logger.Warning(ex, "{Interceptor} failed to intercept {EventName}", interceptor.GetType().Name, typeof(T).Name);
            }
    }
    
    public void Intercept<T>(T evt) where T : InterceptorEvent => Intercept(evt, CancellationToken.None).Synchronously();  

    public IEnumerator<InterceptorModule> GetEnumerator() => Interceptors.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public InterceptorCollection Prepend(InterceptorModule interceptor) {
        Ensure.NotNull(interceptor, nameof(interceptor));

        if (Interceptors.Contains(interceptor))
            throw new ArgumentException($"{interceptor.GetType().Name} already added");

        return this with {
            Interceptors = Interceptors.Prepend(interceptor).ToHashSet()
        };
    }

    public InterceptorCollection Append(InterceptorModule interceptor) {
        Ensure.NotNull(interceptor, nameof(interceptor));

        if (Interceptors.Contains(interceptor))
            throw new ArgumentException($"{interceptor.GetType().Name} already added");

        return this with {
            Interceptors = Interceptors.Append(interceptor).ToHashSet()
        };
    }

    public InterceptorCollection AppendRange(IEnumerable<InterceptorModule> interceptors) => 
        interceptors.Aggregate(this, (collection, interceptor) => collection.Append(interceptor));

    public bool Contains(InterceptorModule interceptor) => Interceptors.Contains(interceptor);

    public int Count => Interceptors.Count;
    
    public async ValueTask DisposeAsync() {
        foreach (var interceptor in Interceptors)
            try {
                await interceptor
                    .DisposeAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception ex) {
                Logger.Warning(ex, "{Interceptor} failed to dispose", interceptor.GetType().Name);
            }
    }
}