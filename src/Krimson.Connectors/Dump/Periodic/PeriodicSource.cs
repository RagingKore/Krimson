// using Krimson.Connectors.Checkpoints;
// using static Serilog.Log;
// using ILogger = Serilog.ILogger;
//
// namespace Krimson.Connectors;
//
// [PublicAPI]
// public class PeriodicSource : IExecutableDataSource<PeriodicSourceModule, PeriodicSourceContext> {
//     static readonly ILogger Log = ForContext<PeriodicSource>();
//
//     protected PeriodicSource(PeriodicSourceOptions options) {
//         Options     = options;
//         Checkpoints = new(options.Reader);
//     }
//
//     PeriodicSourceOptions   Options     { get; }
//     SourceCheckpointManager Checkpoints { get; }
//     
//     public async Task Execute(PeriodicSourceModule module, PeriodicSourceContext context) {
//         try {
//             var isValid = await module.OnValidate(context).ConfigureAwait(false);
//
//             if (!isValid) {
//                 throw new Exception("Validation failed!");
//             }
//             
//             context.CancellationToken.ThrowIfCancellationRequested();
//             
//             var processedRecords = await module.ParseRecords(context)
//                 .WhereAwaitWithCancellation(RecordIsUnseen)
//                 .OrderBy(record => record.EventTime)
//                 .SelectAwaitWithCancellation(Push)
//                 .ToListAsync(context.CancellationToken)
//                 .ConfigureAwait(false);
//             
//             await OnSuccess(processedRecords).ConfigureAwait(false);
//         }
//         catch (Exception ex) {
//             await OnError(ex).ConfigureAwait(false);
//         }
//
//         async ValueTask<bool> RecordIsUnseen(SourceRecord record, CancellationToken ct) {
//             var checkpoint = await Checkpoints
//                 .GetCheckpoint(record.DestinationTopic!, ct)
//                 .ConfigureAwait(false);
//
//             return record.EventTime > checkpoint.Timestamp;
//         }
//
//         async ValueTask OnSuccess(List<SourceRecord> processedRecords) {
//             if (processedRecords.Any()) {
//                 context.Checkpoint = SourceCheckpoint.From(processedRecords.Last());
//                 
//                 Log.Information(
//                     "{RecordCount} record(s) processed up to checkpoint {Checkpoint}",
//                     processedRecords.Count, context.Checkpoint
//                 );
//             }
//
//             try {
//                 await module
//                     .OnSuccess(context, processedRecords)
//                     .ConfigureAwait(false);
//             }
//             catch (Exception ex) {
//                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
//             }
//         }
//         
//         async ValueTask OnError(Exception exception) {
//             try {
//                 await module
//                     .OnError(context, exception)
//                     .ConfigureAwait(false);
//             }
//             catch (Exception ex) {
//                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
//             }
//         }
//     }
//     
//     public async ValueTask<SourceRecord> Push(SourceRecord record, CancellationToken cancellationToken) {
//         await Options.Producer
//             .PushSourceRecord(record)
//             .ConfigureAwait(false);
//             
//         await record
//             .EnsureProcessed()
//             .ConfigureAwait(false);
//
//         return record;
//     }
//
//     public IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken) => 
//         AsyncEnumerable.Empty<SourceRecord>();
//
//     public ValueTask DisposeAsync() => 
//         ValueTask.CompletedTask;
// }