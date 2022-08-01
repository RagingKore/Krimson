namespace Krimson.Connectors;

public interface ISourceConnectorContext {
    public CancellationToken CancellationToken { get; }
}