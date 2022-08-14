namespace Krimson.Connectors;

[PublicAPI]
public abstract class DataSourceModule<TContext> : IDataSourceModule<TContext> where TContext : IDataSourceContext {
    public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);
    
    public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) =>
        ValueTask.CompletedTask;

    public ValueTask OnError(TContext context, Exception exception) =>
        ValueTask.CompletedTask;
}