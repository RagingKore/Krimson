namespace Krimson.Connectors;

public interface ISourceConnector<in TContext, TData> where TContext : ISourceConnectorContext {

    Task Execute(TContext context);
    
    IAsyncEnumerable<TData> SourceData(TContext context);

    IAsyncEnumerable<SourceRecord> SourceRecords(IAsyncEnumerable<TData> data, CancellationToken cancellationToken);

    ValueTask OnSuccess(TContext context, List<ProcessedSourceRecord> processedRecords);

    ValueTask OnError(TContext context, Exception exception);
}