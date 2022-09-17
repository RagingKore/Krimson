using System.Collections.Concurrent;
using Krimson.State;

namespace Krimson.Connectors;

public interface IDataSourceContext {
    IServiceProvider        Services          { get; }
    IStateStore             State             { get; }
    Counter                 Counter           { get; }
    CancellationTokenSource Cancellator       { get; }
    CancellationToken       CancellationToken { get; }
}

public class Counter : IEnumerable<(string Topic, int Count)> {
    ConcurrentDictionary<string, int> CountPerTopic { get; } = new();

    public int Total   => CountPerTopic.Where(x => x.Key != "").Sum(x => x.Value);
    public int Skipped => CountPerTopic.GetOrAdd("", 0);

    public void IncrementSkipped()               => CountPerTopic.AddOrUpdate("", 1, (_, count) => ++count);
    public void IncrementProcessed(string topic) => CountPerTopic.AddOrUpdate(topic, 1, (_, count) => ++count);

    public IEnumerator<(string Topic, int Count)> GetEnumerator() =>
        CountPerTopic
            .Where(x => x.Key != "")
            .Select(x => (x.Key, x.Value))
            .GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}