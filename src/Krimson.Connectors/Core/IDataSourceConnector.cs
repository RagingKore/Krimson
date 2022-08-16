namespace Krimson.Connectors;

public interface IDataSourceConnector<in TContext> where TContext : IDataSourceContext {
    Task Process(TContext context);
}