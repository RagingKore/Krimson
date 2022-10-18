using System.Collections.Concurrent;
using static System.String;

namespace Krimson.Connectors;

public class Counter : IEnumerable<(string Topic, int Count)> {
    ConcurrentDictionary<string, int> CountPerTopic { get; } = new();

    public int Total   => CountPerTopic.Where(x => x.Key != Empty).Sum(x => x.Value);
    public int Skipped => CountPerTopic.GetOrAdd(Empty, 0);

    public void IncrementSkipped()               => CountPerTopic.AddOrUpdate(Empty, 1, (_, count) => ++count);
    public void IncrementProcessed(string topic) => CountPerTopic.AddOrUpdate(topic, 1, (_, count) => ++count);
    public void Reset()                          => CountPerTopic.Clear();
    
    public IEnumerator<(string Topic, int Count)> GetEnumerator() =>
        CountPerTopic
            .Where(x => x.Key != "")
            .Select(x => (x.Key, x.Value))
            .GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}