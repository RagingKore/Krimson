namespace Krimson.Connectors;

public class PeriodicSourceContext : DataSourceContext {
    public PeriodicSourceContext(IServiceProvider services, CancellationToken cancellationToken) : base(services, cancellationToken) { }
}