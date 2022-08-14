namespace Krimson.Connectors;

public interface IExecutableDataSource<in TContext> : IDataSource where TContext : IDataSourceContext {
    Task Execute(TContext context);
}