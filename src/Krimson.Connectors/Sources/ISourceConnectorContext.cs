namespace Krimson.Connectors.Sources;

public interface ISourceConnectorContext {
    public CancellationToken CancellationToken { get; }
}