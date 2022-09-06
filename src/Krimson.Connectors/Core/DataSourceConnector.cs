using Krimson.Connectors.Checkpoints;
using Krimson.Producers;
using Krimson.Readers;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

public delegate ValueTask OnSuccess<in TContext>(TContext context, List<SourceRecord> processedRecords);

public delegate ValueTask OnError<in TContext>(TContext context, Exception exception);

[PublicAPI]
public abstract class DataSourceConnector<TContext> : IDataSourceConnector<TContext> where TContext : IDataSourceContext {
    protected DataSourceConnector() {
        Name = GetType().Name;
        Log  = ForContext(SourceContextPropertyName, Name);
        
        Initialized = new();
        Producer    = null!;
        Checkpoints = null!;

        OnSuccessHandler = (ctx, records) => ValueTask.CompletedTask;
        OnErrorHandler   = (ctx, ex) => ValueTask.CompletedTask;
    }

    protected InterlockedBoolean Initialized { get; }
    protected ILogger            Log         { get; }

    protected KrimsonProducer         Producer    { get; set; }
    protected SourceCheckpointManager Checkpoints { get; set; }
    protected bool                    Synchronous { get; set; }
    
    OnSuccess<TContext> OnSuccessHandler { get; set; }
    OnError<TContext>   OnErrorHandler   { get; set; }

    public string Name { get; }

    public bool IsInitialized => Initialized.CurrentValue;

    public virtual IDataSourceConnector<TContext> Initialize(IServiceProvider services) {
        try {
            if (Initialized.EnsureCalledOnce()) return this;

            Producer    = services.GetRequiredService<KrimsonProducer>();
            Checkpoints = new(services.GetRequiredService<KrimsonReader>());

            return this;
        }
        catch (Exception ex) {
            throw new($"Failed to initialize source connector: {Name}", ex);
        }
    }
    
    public virtual async Task Process(TContext context) {
        Initialize(context.Services);
        
        try {
            var records = await ParseRecords(context)
                .OrderBy(record => record.EventTime)
                .SelectAwaitWithCancellation(ProcessRecord)
                .ToListAsync(context.CancellationToken)
                .ConfigureAwait(false);
    
            await Flush(records).ConfigureAwait(false);
                
            await OnSuccessInternal(records).ConfigureAwait(false);
        }
        catch (Exception ex) {
            await OnErrorInternal(ex).ConfigureAwait(false);
        }

        async Task Flush(List<SourceRecord> sourceRecords) {
            if (!Synchronous) Producer.Flush();

            await Task
                .WhenAll(sourceRecords.Select(x => x.EnsureProcessed()))
                .ConfigureAwait(false);
        }

        async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
            if (processedRecords.Any()) {
                var skipped          = processedRecords.Where(x => x.ProcessingSkipped).ToList();
                var processedByTopic = processedRecords.Where(x => x.ProcessingSuccessful).GroupBy(x => x.DestinationTopic).ToList();
        
                if (skipped.Any()) Log.Information("{RecordCount} record(s) skipped", skipped.Count);

                foreach (var recordSet in processedByTopic) {
                    Checkpoints.TrackCheckpoint(SourceCheckpoint.From(recordSet.Last()));
                    Log.Information("{RecordsCount} record(s) processed | {Topic}", recordSet.Count(), recordSet.Key);
                }
            }
            else
                Log.Information("No source data available to process");

            try {
                await OnSuccessHandler(context, processedRecords).ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} User exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
            }
        }

        async ValueTask OnErrorInternal(Exception exception) {
            Log.Error(exception, "Failed to process source data!");
            
            try {
                await OnErrorHandler(context, exception).ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} User exception: {ErrorMessage}", nameof(OnError), ex.Message);
            }
        }
    }
  
    public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);

    public virtual async ValueTask<SourceRecord> ProcessRecord(SourceRecord record, int index, CancellationToken cancellationToken) {
        // ensure source connector name is set
        record.Source ??= Name;
        
        // ensure destination topic is set
        record.DestinationTopic ??= Producer.Topic;

        if (!record.HasDestinationTopic)
            throw new($"{record.Source} Found record in position {index} with missing destination topic!");

        var isUnseen = await IsRecordUnseen().ConfigureAwait(false);

        if (!isUnseen) {
            record.Skip();
            return record;
        }
        
        var request = ProducerRequest.Builder
            .Key(record.Key)
            .Message(record.Value)
            .Timestamp(record.EventTime)
            .Headers(record.Headers)
            .Topic(record.DestinationTopic)
            .RequestId(record.RequestId)
            .Create();
        
        if (Synchronous)
            HandleResult(record, await Producer.Produce(request, throwOnError: false).ConfigureAwait(false));
        else
            Producer.Produce(request, result => HandleResult(record, result));

        return record;
        
        static void HandleResult(SourceRecord record, ProducerResult result) {
            if (result.Success)
                record.Ack(result.RecordId);
            else
                record.Nak(result.Exception!); //TODO SS: should I trigger the exception right here!?
        }

        async ValueTask<bool> IsRecordUnseen() {
            var checkpoint = await Checkpoints
                .GetCheckpoint(record.DestinationTopic!, cancellationToken)
                .ConfigureAwait(false);

            var unseenRecord = record.EventTime > checkpoint.Timestamp;

            if (!unseenRecord)
                Log.Warning(
                    "{SourceName} Record {RecordIndex} already processed at least once on {EventTime} | Current Checkpoint Timestamp: {CheckpointTimestamp}", 
                    record.Source, index, record.EventTime, checkpoint.Timestamp
                );

            return unseenRecord;
        }
    }
    
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
    
    protected void OnSuccess(OnSuccess<TContext> handler) => 
        OnSuccessHandler = handler ?? throw new ArgumentNullException(nameof(handler));
    
    protected void OnError(OnError<TContext> handler) =>
        OnErrorHandler = handler ?? throw new ArgumentNullException(nameof(handler));
}