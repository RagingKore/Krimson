using System.Threading.Channels;
using Krimson.Connectors.Checkpoints;
using Krimson.Producers;
using Krimson.Readers;
using static System.Threading.Channels.Channel;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

public record DataSourceOptions(int QueueLength = 1000);

[PublicAPI]
public abstract class DataSource<TContext> : IDataSource<TContext> where TContext : IDataSourceContext {
    public const int DefaultQueueLength = 1000;

    protected DataSource(DataSourceOptions? options = null) {
        Channel = CreateBounded<SourceRecord>((options ?? new DataSourceOptions()).QueueLength);
        Log     = ForContext(SourceContextPropertyName, GetType().Name);
    }
    
    Channel<SourceRecord> Channel { get; }

    protected ILogger Log { get; } 

    public virtual async ValueTask<SourceRecord> AddRecord(SourceRecord record, CancellationToken cancellationToken) {
        await Channel.Writer
            .WriteAsync(record, cancellationToken)
            .ConfigureAwait(false);
        
        return record;
    }
    
    public virtual IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken) => 
        Channel.Reader.ReadAllAsync(cancellationToken);
    
    public virtual ValueTask DisposeAsync() {
        Channel.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
    
    public virtual async Task Process(TContext context) {
        try {
            var processedRecords = await ParseRecords(context)
                .OrderBy(record => record.EventTime)
                .SelectAwaitWithCancellation(AddRecord)
                .ToListAsync(context.CancellationToken)
                .ConfigureAwait(false);

            await Task.WhenAll(processedRecords.Select(x => x.EnsureProcessed())).ConfigureAwait(false);

            await OnSuccessInternal(processedRecords).ConfigureAwait(false);
        }
        catch (Exception ex) {
            await OnErrorInternal(ex).ConfigureAwait(false);
        }

        async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
            if (processedRecords.Any()) {
                var lastEventTime = DateTimeOffset.FromUnixTimeMilliseconds(processedRecords.Last().EventTime);
                
                Log.Information(
                    "{RecordCount} record(s) processed up to {Checkpoint:O}",
                    processedRecords.Count, lastEventTime
                );
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

    public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) => ValueTask.CompletedTask;

    public virtual ValueTask OnError(TContext context, Exception exception) => ValueTask.CompletedTask;
}

public abstract class DirectDataSource<TContext> : DataSource<TContext> where TContext : IDataSourceContext {
    protected DirectDataSource(KrimsonProducer producer, KrimsonReader reader) {
        Producer    = producer;
        Checkpoints = new(reader);
    }
    
    KrimsonProducer          Producer    { get; }
    SourceCheckpointManager  Checkpoints { get; }

    public override async ValueTask<SourceRecord> AddRecord(SourceRecord record, CancellationToken cancellationToken) {
        var checkpoint = await Checkpoints
            .GetCheckpoint(record.DestinationTopic!, cancellationToken)
            .ConfigureAwait(false);

        if (record.EventTime > checkpoint.Timestamp) {
            await Producer
                .SendSourceRecord(record)
                .ConfigureAwait(false);
        }

        return record;
    }
}