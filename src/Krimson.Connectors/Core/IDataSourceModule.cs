namespace Krimson.Connectors;

public interface IDataSourceModule<in TContext> where TContext : IDataSourceContext {
    // ValueTask<bool> OnValidate(TContext context);
    
    IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);

    ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords);
    
    ValueTask OnError(TContext context, Exception exception);
}