namespace Krimson.State;

public interface IStateStore {
    ValueTask     Set<T>(object key, T value, CancellationToken cancellationToken = default);
    ValueTask<T?> Get<T>(object key, CancellationToken cancellationToken = default);
    ValueTask     Delete(object key, CancellationToken cancellationToken = default);
    ValueTask<T?> GetOrSet<T>(object key, Func<ValueTask<T>> factory, CancellationToken cancellationToken = default);
}