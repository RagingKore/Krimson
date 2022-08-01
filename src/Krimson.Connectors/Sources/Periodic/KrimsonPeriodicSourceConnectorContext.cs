namespace Krimson.Connectors.Sources;

[PublicAPI]
public class KrimsonPeriodicSourceConnectorContext : ISourceConnectorContext {
    internal KrimsonPeriodicSourceConnectorContext(IServiceProvider services, CancellationToken cancellationToken) {
        Services          = services;
        CancellationToken = cancellationToken;
        Checkpoint        = Checkpoint.None;
    }
    
    internal IServiceProvider  Services          { get; }
    public   CancellationToken CancellationToken { get; }
    public   Checkpoint        Checkpoint        { get; internal set; }
}