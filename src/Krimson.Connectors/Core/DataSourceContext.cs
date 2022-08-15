using Krimson.State;

namespace Krimson.Connectors;

[PublicAPI]
public class DataSourceContext : IDataSourceContext {
    public DataSourceContext(IServiceProvider services, CancellationToken cancellationToken) {
        Services          = services;
        CancellationToken = cancellationToken;
        State             = services.GetRequiredService<IStateStore>();
    }

    public IServiceProvider  Services          { get; }
    public IStateStore       State             { get; }
    public CancellationToken CancellationToken { get; }
}