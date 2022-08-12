namespace Krimson.Connectors;

public interface IDataSourceOptions { }

public interface IDataSource : IAsyncDisposable {
    public ValueTask<SourceRecord>        Push(SourceRecord record, CancellationToken cancellationToken);
    public IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken);
}