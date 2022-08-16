using Krimson.Connectors.Checkpoints;
using Krimson.Producers;
using Krimson.Readers;
using static System.DateTimeOffset;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

[PublicAPI]
public abstract class DataSourceConnector<TContext> : IDataSourceConnector<TContext> where TContext : IDataSourceContext {
    protected DataSourceConnector() {
        Name        = GetType().Name;
        Log         = ForContext(SourceContextPropertyName, Name);
        Initialized = new();
        Producer    = null!;
        Checkpoints = null!;
    }

    protected InterlockedBoolean Initialized { get; }
    protected ILogger            Log         { get; }

    protected KrimsonProducer         Producer    { get; set; }
    protected SourceCheckpointManager Checkpoints { get; set; }
    protected bool                    Synchronous { get; set; }

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
        
                if (skipped.Any()) Log.Debug("{RecordCount} record(s) skipped", skipped.Count);

                foreach (var recordSet in processedByTopic) {
                    var lastRecord = recordSet.Last();
               
                    Checkpoints.TrackCheckpoint(SourceCheckpoint.From(lastRecord));

                    var recordCount = recordSet.Count();
                
                    Log.Information(
                        "{RecordsCount} record(s) processed up to checkpoint {Topic} [{Partition}] @ {Offset} :: {EventTime:O}",
                        recordCount, lastRecord.RecordId.Topic, lastRecord.RecordId.Partition,
                        lastRecord.RecordId.Offset, FromUnixTimeMilliseconds(lastRecord.EventTime)
                    );
                }
            }
            
            try {
                await OnSuccess(context, processedRecords).ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
            }
        }

        async ValueTask OnErrorInternal(Exception exception) {
            try {
                await OnError(context, exception).ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
            }
        }
    }
  
    public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);

    public virtual async ValueTask<SourceRecord> ProcessRecord(SourceRecord record, CancellationToken cancellationToken) {
        // ensure source connector name is set
        record.Source ??= Name;
        
        // ensure destination topic is set
        record.DestinationTopic ??= Producer.Topic;

        if (!record.HasDestinationTopic)
            throw new($"{Name} found record with missing destination topic!");

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
                Log.Verbose(
                    "{SourceName} | record already processed at least once on {EventTime} | checkpoint: {CheckpointTimestamp}", 
                    record.Source, record.EventTime, checkpoint.Timestamp
                );

            return unseenRecord;
        }
    }
  
    public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) => ValueTask.CompletedTask;

    public virtual ValueTask OnError(TContext context, Exception exception) => ValueTask.CompletedTask;
    
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
}