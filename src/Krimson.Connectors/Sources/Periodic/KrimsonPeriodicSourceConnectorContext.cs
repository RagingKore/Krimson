using Krimson.State;

namespace Krimson.Connectors;

[PublicAPI]
public class KrimsonPeriodicSourceConnectorContext : ISourceConnectorContext {
    internal KrimsonPeriodicSourceConnectorContext(IServiceProvider services, CancellationToken cancellationToken) {
        Services          = services;
        CancellationToken = cancellationToken;
        State             = services.GetRequiredService<IStateStore>(); // hello darkness my old friend...
        Checkpoint        = Checkpoint.None;
    }

    internal IServiceProvider  Services          { get; }
    public   IStateStore       State             { get; }
    public   CancellationToken CancellationToken { get; }
    public   Checkpoint        Checkpoint        { get; internal set; }
    
    public T GetService<T>() where T : notnull => Services.GetRequiredService<T>();
}