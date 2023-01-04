using Confluent.Kafka;
using Krimson.Connectors.Checkpoints;
using Krimson.Producers;
using Krimson.Readers;
using Microsoft.Extensions.DependencyInjection;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

public delegate ValueTask OnSuccess<in TContext>(TContext context);

public delegate ValueTask OnError<in TContext>(TContext context, Exception exception);

public enum DataSourceCheckpointStrategy {
    Manual,
    Record,
    Batch
}

[PublicAPI]
public abstract class DataSourceConnector<TContext> : IDataSourceConnector<TContext> where TContext : IDataSourceContext {
    protected DataSourceConnector() {
        Name = GetType().Name;
        Log  = ForContext(SourceContextPropertyName, Name);
        
        Initialized        = new();
        Producer           = null!;
        Checkpoints        = null!;
        CheckpointStrategy = DataSourceCheckpointStrategy.Record;

        OnSuccessHandler = _ => ValueTask.CompletedTask;
        OnErrorHandler   = (_, _) => ValueTask.CompletedTask;
    }

    protected InterlockedBoolean Initialized { get; }
    protected ILogger            Log         { get; }

    protected DataSourceCheckpointStrategy CheckpointStrategy { get; set; }
    protected KrimsonProducer              Producer           { get; private set; }
    protected SourceCheckpointManager      Checkpoints        { get; private set; }
    protected bool                         Synchronous        { get; private set; }
    
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
            var checkpoint = SourceCheckpoint.None;

            await foreach (var record in ParseRecords(context).WithCancellation(context.CancellationToken).ConfigureAwait(false)) {
                await ProcessRecord(
                        record,
                        ack: recordId => {
                            record.Ack(recordId);

                            checkpoint = SourceCheckpoint.From(record);

                            if (CheckpointStrategy == DataSourceCheckpointStrategy.Record)
                                Checkpoints.TrackCheckpoint(checkpoint);

                            context.TrackRecord(record);
                            context.Counter.IncrementProcessed(record.DestinationTopic!);

                            //Log.Verbose("{RequestId} record acknowledged", record.RequestId);
                        },
                        nack: exception => {
                            record.Nak(exception);
                            OnErrorInternal(exception).AsTask().GetAwaiter().GetResult();
                        },
                        skip: () => {
                            context.TrackRecord(record);
                            context.Counter.IncrementSkipped();
                        }
                    )
                    .ConfigureAwait(false);
            }

            if (!Synchronous) Producer.Flush();
            
            if (CheckpointStrategy == DataSourceCheckpointStrategy.Batch) 
                Checkpoints.TrackCheckpoint(checkpoint);
            
            await OnSuccessInternal().ConfigureAwait(false);
        }
        catch (Exception ex) {
            await OnErrorInternal(ex).ConfigureAwait(false);
        }

        async ValueTask OnSuccessInternal() {
            if (context.Counter.Skipped > 0)
                Log.Information("{RecordsCount} record(s) skipped", context.Counter.Skipped);

            foreach (var (topic, count) in context.Counter) 
                Log.Information("{RecordsCount} record(s) processed >> {Topic}", count, topic);

            try {
                await OnSuccessHandler(context).ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} User exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
            }
            finally {
                context.Records.Clear();
                context.Counter.Reset();
            }
        }

        async ValueTask OnErrorInternal(Exception exception) {
            context.Cancellator.Cancel();
            
            Log.Error(exception, "Failed to process source data!");
            
            try {
                await OnErrorHandler(context, exception).ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} User exception: {ErrorMessage}", nameof(OnError), ex.Message);
            }
            finally {
                context.Counter.Reset();
            }
        }
    }
  
    public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);

    public async ValueTask ProcessRecord(SourceRecord record, Action<RecordId> ack, Action<ProduceException<byte[], object?>> nack, Action skip) {
        // ensure source connector name is set
        record.Source ??= Name;
        
        // ensure destination topic is set
        record.DestinationTopic ??= Producer.Topic;

        if (record.ProcessingSkipped) {
            skip();
            Log.Debug("{SourceName} {RequestId} record skipped by user", record.Source, record.RequestId);
            return;
        }

        if (!record.HasDestinationTopic)
            throw new($"{record.Source} {record.RequestId} record is missing destination topic!");

        if (CheckpointStrategy != DataSourceCheckpointStrategy.Manual) {
            var isUnseen = await IsRecordUnseen().ConfigureAwait(false);

            if (!isUnseen) {
                skip();
                return;
            }    
        }
        
        var request = ProducerRequest.Builder
            .Key(record.Key)
            .Message(record.Value)
            .Timestamp(record.EventTime)
            .Headers(record.Headers)
            .Topic(record.DestinationTopic)
            .RequestId(record.RequestId)
            .Create();
        
        Producer.Produce(request, result => {
            if (result.Success)
                ack(result.RecordId);
            else
                nack(result.Exception!);
        });

        async ValueTask<bool> IsRecordUnseen() {
            if (record == SourceRecord.Empty)
                return false;
            
            var checkpoint = await Checkpoints
                .GetCheckpoint(record.DestinationTopic!, CancellationToken.None)
                .ConfigureAwait(false);

            var unseenRecord = record.EventTime > checkpoint.Timestamp;

            if (!unseenRecord)
                Log.Debug(
                    "{SourceName} {RequestId} record already processed at least once on {EventTime} | Current Checkpoint Timestamp: {CheckpointTimestamp}",
                    record.Source, record.RequestId, record.EventTime, checkpoint.Timestamp
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