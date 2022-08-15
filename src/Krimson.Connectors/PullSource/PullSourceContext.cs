namespace Krimson.Connectors;

public class PullSourceContext : DataSourceContext {
    public PullSourceContext(IServiceProvider services, CancellationToken cancellationToken) : base(services, cancellationToken) { }
}