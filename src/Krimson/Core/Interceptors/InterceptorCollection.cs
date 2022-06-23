using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Krimson.Interceptors;

// [PublicAPI]
// public sealed class InterceptorCollection : ICollection<InterceptorModule> {
//     public InterceptorCollection(ILogger? logger = null) {
//         Logger = logger ?? NullLogger.Instance;
//     }
//
//     public InterceptorCollection(IEnumerable<InterceptorModule> interceptors, ILogger? logger = null): this(logger) {
//         foreach (var interceptor in interceptors)
//             Add(interceptor);
//     }
//     
//     ILogger                    Logger                 { get; }
//     HashSet<InterceptorModule> RegisteredInterceptors { get; set; } = new();
//
//     public async Task Intercept<T>(T evt, CancellationToken cancellationToken) where T : InterceptorEvent {
//         foreach (var interceptor in RegisteredInterceptors)
//             try {
//                 await interceptor
//                     .Intercept(evt, cancellationToken)
//                     .ConfigureAwait(false);
//             }
//             catch (Exception ex) {
//                 Logger.LogWarning(ex, "{Interceptor} failed to intercept {EventName}", interceptor.GetType().Name, typeof(T).Name);
//             }
//     }
//     
//     public void Intercept<T>(T evt) where T : InterceptorEvent => Intercept(evt, CancellationToken.None).Synchronously();  
//
//     public IEnumerator<InterceptorModule> GetEnumerator() => RegisteredInterceptors.GetEnumerator();
//
//     IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
//
//     public void Add(InterceptorModule interceptor) {
//         Ensure.NotNull(interceptor, nameof(interceptor));
//
//         if (!RegisteredInterceptors.Add(interceptor))
//             throw new ArgumentException($"{interceptor.GetType().Name} already added");
//     }
//
//     public InterceptorCollection AddFirst(InterceptorModule interceptor) {
//         Ensure.NotNull(interceptor, nameof(interceptor));
//
//         if (RegisteredInterceptors.Contains(interceptor))
//             throw new ArgumentException($"{interceptor.GetType().Name} already added");
//
//         return new(interceptor.Concat(RegisteredInterceptors).ToHashSet());
//     }
//
//     public InterceptorCollection AddLast(InterceptorModule interceptor) {
//         Ensure.NotNull(interceptor, nameof(interceptor));
//
//         if (RegisteredInterceptors.Contains(interceptor))
//             throw new ArgumentException($"{interceptor.GetType().Name} already added");
//
//         return new(RegisteredInterceptors.Append(interceptor).ToHashSet());
//     }
//
//     public void Clear() => RegisteredInterceptors.Clear();
//
//     public bool Contains(InterceptorModule interceptor)           => RegisteredInterceptors.Contains(interceptor);
//     public void CopyTo(InterceptorModule[] array, int arrayIndex) => RegisteredInterceptors.CopyTo(array, arrayIndex);
//     public bool Remove(InterceptorModule interceptor)             => RegisteredInterceptors.Remove(interceptor);
//
//     public int  Count      => RegisteredInterceptors.Count;
//     public bool IsReadOnly => false;
// }

[PublicAPI]
public record InterceptorCollection : IReadOnlyCollection<InterceptorModule> {
    public InterceptorCollection(ILoggerFactory? loggerFactory = null) {
        LoggerFactory = loggerFactory ?? new NullLoggerFactory();
        Logger        = LoggerFactory.CreateLogger(nameof(Interceptors));
        Interceptors  = new();
    }

    HashSet<InterceptorModule> Interceptors  { get; init; }
    ILoggerFactory             LoggerFactory { get; init; }
    ILogger                    Logger        { get; init; }
    
    public async Task Intercept<T>(T evt, CancellationToken cancellationToken) where T : InterceptorEvent {
        foreach (var interceptor in Interceptors)
            try {
                await interceptor
                    .Intercept(evt, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) {
                Logger.LogWarning(ex, "{Interceptor} failed to intercept {EventName}", interceptor.GetType().Name, typeof(T).Name);
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

    public InterceptorCollection AppendRange(IEnumerable<InterceptorModule> interceptors) {
        return interceptors.Aggregate(this, (collection, interceptor) => collection.Append(interceptor));
    }

    public bool Contains(InterceptorModule interceptor) => Interceptors.Contains(interceptor);
    
    public InterceptorCollection WithLoggerFactory(ILoggerFactory loggerFactory) {
        Ensure.NotNull(loggerFactory, nameof(loggerFactory));

        return Interceptors
            .Aggregate(
                new InterceptorCollection {
                    LoggerFactory = loggerFactory,
                    Logger        = loggerFactory.CreateLogger(nameof(Interceptors))
                }, 
                (seed, interceptor) => seed.Append(interceptor.With(x => x.SetLoggerFactory(seed.LoggerFactory)))
            );
    }
    
    public int Count => Interceptors.Count;
}