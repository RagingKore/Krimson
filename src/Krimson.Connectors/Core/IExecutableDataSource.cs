namespace Krimson.Connectors;

public interface IExecutableDataSource<in TModule, in TContext> : IDataSource
    where TModule : IDataSourceModule<TContext>
    where TContext : IDataSourceContext {
    public Task Execute(TModule module, TContext context);

    void Initialize(IServiceProvider services);
}