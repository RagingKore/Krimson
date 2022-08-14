namespace Krimson.Connectors;

public interface IDataSourceModule<in TContext> where TContext : IDataSourceContext {
    IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);

    ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords);
    
    ValueTask OnError(TContext context, Exception exception);
}