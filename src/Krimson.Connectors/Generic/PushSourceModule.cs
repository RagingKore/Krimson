namespace Krimson.Connectors;

[PublicAPI]
public abstract class PushSourceModule<TContext> : IDataSourceModule<TContext> where TContext : IDataSourceContext {
    public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);

    public virtual ValueTask<bool> OnValidate(TContext context) => 
        ValueTask.FromResult(true);
    
    public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) =>
        ValueTask.CompletedTask;

    public ValueTask OnError(TContext context, Exception exception) =>
        ValueTask.CompletedTask;
}