namespace Krimson.Connectors;

public interface IDataSource : IAsyncDisposable {
    ValueTask<SourceRecord> Push(SourceRecord record, CancellationToken cancellationToken);

    IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken);

    // void Initialize(IServiceProvider services);
}

public interface IExecutableDataSource<in TContext> : IDataSource where TContext : IDataSourceContext {
    Task Execute(TContext context);
}