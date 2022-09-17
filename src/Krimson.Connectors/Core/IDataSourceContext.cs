using Krimson.State;

namespace Krimson.Connectors;

public interface IDataSourceContext {
    IServiceProvider  Services          { get; }
    IStateStore       State             { get; }
    CancellationToken CancellationToken { get; }
    Counter           Counter           { get; }
}

public class Counter : IEnumerable<(string Topic, int Count)> {
    Dictionary<string, int> CountPerTopic { get; } = new Dictionary<string, int>();

    public int Total   => CountPerTopic.Values.Sum();
    public int Skipped => CountPerTopic[""];
    
    public void IncrementSkipped()               => CountPerTopic[""]++;
    public void IncrementProcessed(string topic) => CountPerTopic[topic]++;

    public IEnumerator<(string Topic, int Count)> GetEnumerator() =>
        CountPerTopic
            .Where(x => x.Key != "")
            .Select(x => (x.Key!, x.Value))
            .GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}