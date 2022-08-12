using Krimson.Connectors.Checkpoints;
using Krimson.Producers;
using Krimson.Readers;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

[PublicAPI]
public abstract class PushSource<TModule, TContext> : IExecutableDataSource<TModule, TContext> where TModule : IDataSourceModule<TContext> where TContext : IDataSourceContext {
    protected PushSource() {
        Log = ForContext(SourceContextPropertyName, GetType().Name);
    }
    
    // protected PushSource(PushSourceOptions options) {
    //     Options     = options;
    //     Checkpoints = new(options.Reader);
    //     Log         = ForContext(SourceContextPropertyName, GetType().Name);
    // }

    protected ILogger       Log         { get; }
    PushSourceOptions       Options     { get; set; }
    SourceCheckpointManager Checkpoints { get; set; }

    public virtual void Initialize(IServiceProvider services) {
        Options     = new(services.GetRequiredService<KrimsonReader>(), services.GetRequiredService<KrimsonProducer>());
        Checkpoints = new(Options.Reader);
    }

    public virtual async Task Execute(TModule module, TContext context) {
        try {
            // var isValid = await module.OnValidate(context).ConfigureAwait(false);
            //
            // if (!isValid) {
            //     throw new Exception("Validation failed!");
            // }
            //
            // context.CancellationToken.ThrowIfCancellationRequested();
            
            var processedRecords = await module.ParseRecords(context)
                .WhereAwaitWithCancellation(RecordIsUnseen)
                .OrderBy(record => record.EventTime)
                .SelectAwaitWithCancellation(Push)
                .ToListAsync(context.CancellationToken)
                .ConfigureAwait(false);
            
            await OnSuccess(processedRecords).ConfigureAwait(false);
        }
        catch (Exception ex) {
            await OnError(ex).ConfigureAwait(false);
        }

        async ValueTask<bool> RecordIsUnseen(SourceRecord record, CancellationToken ct) {
            var checkpoint = await Checkpoints
                .GetCheckpoint(record.DestinationTopic!, ct)
                .ConfigureAwait(false);

            return record.EventTime > checkpoint.Timestamp;
        }

        async ValueTask OnSuccess(List<SourceRecord> processedRecords) {
            if (processedRecords.Any()) {
                context.SetCheckpoint(SourceCheckpoint.From(processedRecords.Last()));
                
                Log.Information(
                    "{RecordCount} record(s) processed up to checkpoint {Checkpoint}",
                    processedRecords.Count, context.Checkpoint
                );
            }

            try {
                await module
                    .OnSuccess(context, processedRecords)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
            }
        }
        
        async ValueTask OnError(Exception exception) {
            try {
                await module
                    .OnError(context, exception)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) {
                Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
            }
        }
    }
    
    public async ValueTask<SourceRecord> Push(SourceRecord record, CancellationToken cancellationToken) {
        await Options.Producer
            .PushSourceRecord(record)
            .ConfigureAwait(false);
            
        await record
            .EnsureProcessed()
            .ConfigureAwait(false);

        return record;
    }

    public IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken) => 
        AsyncEnumerable.Empty<SourceRecord>();

    public ValueTask DisposeAsync() => 
        ValueTask.CompletedTask;
}

// public abstract class PeriodicPushSource<TModule, TContext> : PushSource<TModule, TContext> where TModule : IDataSourceModule<TContext> where TContext : IDataSourceContext {
//     public override async Task Execute(TModule module, TContext context) {
//         var backoffTime = GetBackoffTime<TModule>();
//         
//         while (!context.CancellationToken.IsCancellationRequested) {
//             try {
//                 await base.Execute(module, context).ConfigureAwait(false);
//                 await Task.Delay(backoffTime, context.CancellationToken).ConfigureAwait(false);
//             }
//             catch (OperationCanceledException) {
//                 // be kind and don't crash on cancellation
//                 //Log.Debug("stopped on user cancellation request");
//             }
//         }
//         
//         static TimeSpan GetBackoffTime<T>() =>
//             (BackOffTimeAttribute?) Attribute.GetCustomAttribute(typeof(T), typeof(BackOffTimeAttribute)) ?? TimeSpan.FromSeconds(30);
//     }
// }

public interface IAutonomousPushSource<in TContext> where TContext : IDataSourceContext {
    void Initialize(IServiceProvider services);
    Task Execute(TContext context);
}

public interface IAutonomousPushSource : IAutonomousPushSource<IDataSourceContext> { }

public abstract class AutonomousPushSource<TContext> : PushSource<IDataSourceModule<TContext>, TContext>, IDataSourceModule<TContext>, IAutonomousPushSource<TContext> where TContext : IDataSourceContext {
    public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);

    // public virtual ValueTask<bool> OnValidate(TContext context) => 
    //     ValueTask.FromResult(true);
    
    public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) =>
        ValueTask.CompletedTask;

    public ValueTask OnError(TContext context, Exception exception) =>
        ValueTask.CompletedTask;

    public virtual Task Execute(TContext context) => base.Execute(this, context);
}

public abstract class AutonomousPeriodicPushSource<TContext> : AutonomousPushSource<TContext> where TContext : IDataSourceContext {
    public override async Task Execute(IDataSourceModule<TContext> module, TContext context) {
        var backoffTime = GetBackoffTime(GetType());
        
        while (!context.CancellationToken.IsCancellationRequested) {
            try {
                await base.Execute(module, context).ConfigureAwait(false);
                await Task.Delay(backoffTime, context.CancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) {
                // be kind and don't crash on cancellation
                //Log.Debug("stopped on user cancellation request");
            }
        }
        
        static TimeSpan GetBackoffTime(Type type) =>
            (BackOffTimeAttribute?) Attribute.GetCustomAttribute(type, typeof(BackOffTimeAttribute)) ?? TimeSpan.FromSeconds(30);
    }
}

public abstract class AutonomousPeriodicPushSource : AutonomousPushSource<PushSourceContext> { }



//
// public abstract class AutonomousPushSource<TModule, TContext> : PushSource<TModule, TContext>, IDataSourceModule<TContext> where TContext : IDataSourceContext where TModule : IDataSourceModule<TContext> {
//     public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);
//
//     public virtual ValueTask<bool> Validate(TContext context) => 
//         ValueTask.FromResult(true);
//     
//     public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) =>
//         ValueTask.CompletedTask;
//
//     public ValueTask OnError(TContext context, Exception exception) =>
//         ValueTask.CompletedTask;
// }
//
// public abstract class AutonomousPeriodicPushSource<TModule, TContext>  : AutonomousPushSource<TModule, TContext>  where TContext : IDataSourceContext where TModule : IDataSourceModule<TContext> {
//     public override async Task Execute(TModule module, TContext context) {
//         var backoffTime = GetBackoffTime<TModule>();
//         
//         while (!context.CancellationToken.IsCancellationRequested) {
//             try {
//                 await base.Execute(module, context).ConfigureAwait(false);
//                 await Task.Delay(backoffTime, context.CancellationToken).ConfigureAwait(false);
//             }
//             catch (OperationCanceledException) {
//                 // be kind and don't crash on cancellation
//                 //Log.Debug("stopped on user cancellation request");
//             }
//         }
//         
//         static TimeSpan GetBackoffTime<T>() =>
//             (BackOffTimeAttribute?) Attribute.GetCustomAttribute(typeof(T), typeof(BackOffTimeAttribute)) ?? TimeSpan.FromSeconds(30);
//     }
// }