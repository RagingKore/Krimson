namespace Krimson.Connectors;

public interface IDataSource : IAsyncDisposable {
    ValueTask<SourceRecord> AddRecord(SourceRecord record, CancellationToken cancellationToken);

    IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken);
}

public interface IDataSource<in TContext> : IDataSource where TContext : IDataSourceContext {
    Task Process(TContext context);
}