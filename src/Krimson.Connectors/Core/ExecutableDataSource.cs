using Krimson.Connectors.Checkpoints;
using Krimson.Producers;
using Krimson.Readers;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

[PublicAPI]
public abstract class ExecutableDataSource<TContext> : DataSource, IExecutableDataSource<TContext> where TContext : IDataSourceContext {
    protected ExecutableDataSource() => Log = ForContext(SourceContextPropertyName, GetType().Name);

    protected ILogger Log { get; }

    SourceCheckpointManager? Checkpoints { get; set; }

    public virtual async Task Execute(TContext context) {
        Checkpoints ??= new(context.Services.GetRequiredService<KrimsonReader>());
        
        try {
            var processedRecords = await ParseRecords(context)
                .WhereAwaitWithCancellation(RecordIsUnseen)
                .OrderBy(record => record.EventTime)
                .SelectAwaitWithCancellation(Push)
                .ToListAsync(context.CancellationToken)
                .ConfigureAwait(false);

            await Task.WhenAll(processedRecords.Select(x => x.EnsureProcessed())).ConfigureAwait(false);

            await OnSuccessInternal(processedRecords).ConfigureAwait(false);
        }
        catch (Exception ex) {
            await OnErrorInternal(ex).ConfigureAwait(false);
        }

        async ValueTask<bool> RecordIsUnseen(SourceRecord record, CancellationToken ct) {
            var checkpoint = await Checkpoints
                .GetCheckpoint(record.DestinationTopic!, ct)
                .ConfigureAwait(false);

            return record.EventTime > checkpoint.Timestamp;
        }

        async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
            if (processedRecords.Any()) {
                context.SetCheckpoint(SourceCheckpoint.From(processedRecords.Last()));

                Log.Information(
                    "{RecordCount} record(s) processed up to checkpoint {Checkpoint}",
                    processedRecords.Count, context.Checkpoint
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

// [PublicAPI]
// public abstract class AutonomousExecutableDataSource<TContext> : ExecutableDataSource<TContext> where TContext : IDataSourceContext {
//     KrimsonProducer Producer            { get; set; }
//     bool            SynchronousDelivery { get; set; } = true;
//
//     public new virtual void Initialize(IServiceProvider services) {
//         Producer = services.GetRequiredService<KrimsonProducer>();
//         base.Initialize(services);
//     }
//     
//     public override async ValueTask<SourceRecord> Push(SourceRecord record, CancellationToken cancellationToken) {
//         Producer = services.GetRequiredService<KrimsonProducer>(); 
//         
//         await Producer
//              .PushSourceRecord(record, SynchronousDelivery)
//              .ConfigureAwait(false);
//
//          // await record
//          //     .EnsureProcessed()
//          //     .ConfigureAwait(false);
//
//          return record;
//      }
//
//      public override IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken) => AsyncEnumerable.Empty<SourceRecord>();
// }