using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

[PublicAPI]
public abstract class ExecutableDataSource<TContext> : DataSource, IExecutableDataSource<TContext> where TContext : IDataSourceContext {
    protected ExecutableDataSource() => Log = ForContext(SourceContextPropertyName, GetType().Name);

    protected ILogger Log { get; }

    public virtual async Task Execute(TContext context) {
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