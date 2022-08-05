using Krimson.State;

namespace Krimson.Connectors;

public interface ISourceConnectorContext {
    public IStateStore       State             { get; }
    public CancellationToken CancellationToken { get; }
}