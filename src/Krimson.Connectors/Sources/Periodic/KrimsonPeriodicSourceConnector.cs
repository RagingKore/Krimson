using System.Text.Json.Nodes;
using Krimson.Producers;
using Krimson.Readers;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

[PublicAPI]
public abstract class KrimsonPeriodicSourceConnector<TData>: IKrimsonPeriodicSourceConnector<KrimsonPeriodicSourceConnectorContext,TData> {
    protected KrimsonPeriodicSourceConnector() => Log = ForContext(SourceContextPropertyName, GetType().Name);

    protected KrimsonPeriodicSourceConnector(TimeSpan? backoffTime = null) {
        BackoffTime = backoffTime ?? GetBackoffTimeFromAttribute();
        
        Log = ForContext(SourceContextPropertyName, GetType().Name);

        TimeSpan GetBackoffTimeFromAttribute() => 
            (BackOffTimeAttribute?)Attribute.GetCustomAttribute(GetType(), typeof(BackOffTimeAttribute)) ?? TimeSpan.FromSeconds(30);
    }
    
    protected ILogger Log { get; }
    
    public TimeSpan BackoffTime { get; }
    
    public async Task Execute(KrimsonPeriodicSourceConnectorContext context) {
        var reader   = context.Services.GetRequiredService<KrimsonReader>();
        var producer = context.Services.GetRequiredService<KrimsonProducer>();

        // load and set checkpoint
        context.Checkpoint = await LoadCheckpoint(context.CancellationToken).ConfigureAwait(false);

        while (!context.CancellationToken.IsCancellationRequested) {
            try {
                var data = SourceData(context);

                var processedRecords = await SourceRecords(data, context.CancellationToken)
                    .Where(record => !record.Equals(SourceRecord.Empty))
                    // ReSharper disable once AccessToModifiedClosure
                    .Where(record => record.Timestamp.UnixTimestampMs > context.Checkpoint.Timestamp.UnixTimestampMs)
                    .OrderBy(record => record.Timestamp)
                    .SelectAwait(
                        async record => {
                            var result = await producer.Produce(record, record.Key).ConfigureAwait(false);
                            return new ProcessedSourceRecord(record, result.RecordId);
                        }
                    )
                    .ToListAsync(context.CancellationToken)
                    .ConfigureAwait(false);
                
                await OnSuccess(context, processedRecords).ConfigureAwait(false);

                await Task.Delay(BackoffTime, context.CancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) {
                // be kind and don't crash on cancellation
                Log.Debug("stopped on user cancellation request");
            }
            catch (Exception ex) {
                await OnError(context, ex).ConfigureAwait(false);
            }
        }
        
        async ValueTask<Checkpoint> LoadCheckpoint(CancellationToken cancellationToken) {
            Log.Verbose("loading checkpoint...");

            if (producer.Topic is not null)
                return await reader
                    .LoadCheckpoint(producer.Topic, cancellationToken)
                    .ConfigureAwait(false);

            Log.Information("checkpoint not set since producer has no default topic");
        
            return Checkpoint.None;
        }
    }
    
    public abstract IAsyncEnumerable<TData> SourceData(KrimsonPeriodicSourceConnectorContext context);
    
    public virtual IAsyncEnumerable<SourceRecord> SourceRecords(IAsyncEnumerable<TData> data, CancellationToken cancellationToken) {
        return data.Select(node => {
            try {
                return ParseSourceRecord(node);
            }
            catch (Exception ex) {
                Log.Error(ex, "Failed to parse source record!");
                return SourceRecord.Empty;
            }
        });
    }

    public abstract SourceRecord ParseSourceRecord(TData node);

    public ValueTask OnSuccess(KrimsonPeriodicSourceConnectorContext context, List<ProcessedSourceRecord> processedRecords) {
        if (processedRecords.Any()) {
            context.Checkpoint = Checkpoint.From(processedRecords.Last());

            Log.Information(
                "{RecordCount} record(s) processed up to checkpoint {Checkpoint} ",
                processedRecords.Count, context.Checkpoint
            );
        }
        
        return ValueTask.CompletedTask;
    }

    public ValueTask OnError(KrimsonPeriodicSourceConnectorContext context, Exception exception) {
        Log.Error(exception, "connector failed");
        return ValueTask.CompletedTask;
    }
}

[PublicAPI]
public abstract class KrimsonPeriodicSourceConnector : KrimsonPeriodicSourceConnector<JsonNode> { }