using Krimson.State;

namespace Krimson.Connectors;

[PublicAPI]
public class DataSourceContext : IDataSourceContext {
    public DataSourceContext(IServiceProvider services, CancellationToken cancellationToken) {
        Services    = services;
        Cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        State       = services.GetService<IStateStore>() ?? new InMemoryStateStore();
        Counter     = new();
    }

    public IServiceProvider        Services    { get; }
    public IStateStore             State       { get; }
    public Counter                 Counter     { get; }
    public CancellationTokenSource Cancellator { get; }

    public CancellationToken CancellationToken => Cancellator.Token;
}