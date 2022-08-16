// using Krimson.Connectors.Checkpoints;
// using Krimson.Producers;
// using Krimson.Readers;
// using static System.DateTimeOffset;
// using static Serilog.Core.Constants;
// using static Serilog.Log;
// using ILogger = Serilog.ILogger;
//
// namespace Krimson.Connectors;
//
// // public record DataSourceOptions(int QueueLength = 1000);
// //
// // [PublicAPI]
// // public abstract class DataSource<TContext> : IDataSource<TContext> where TContext : IDataSourceContext {
// //     public const int DefaultQueueLength = 1000;
// //
// //     protected DataSource(DataSourceOptions? options = null) {
// //         Channel = CreateBounded<SourceRecord>((options ?? new DataSourceOptions()).QueueLength);
// //         Name    = GetType().Name;
// //         Log     = ForContext(SourceContextPropertyName, Name);
// //     }
// //     
// //     Channel<SourceRecord> Channel { get; }
// //
// //     protected string  Name { get; }
// //     protected ILogger Log  { get; }
// //
// //     public virtual async ValueTask<SourceRecord> AddRecord(SourceRecord record, CancellationToken cancellationToken) {
// //         record.Source = Name;
// //       
// //         if (Channel.Reader.Count == DefaultQueueLength)
// //             Log.Warning("internal queue full");
// //
// //         await Channel.Writer
// //             .WriteAsync(record, cancellationToken)
// //             .ConfigureAwait(false);
// //         
// //         return record;
// //     }
// //
// //     public virtual async IAsyncEnumerable<SourceRecord> Records([EnumeratorCancellation] CancellationToken cancellationToken) {
// //         await foreach (var record in Channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)) {
// //             yield return record;
// //         }
// //     }
// //
// //     public virtual ValueTask DisposeAsync() {
// //         Channel.Writer.TryComplete();
// //         return ValueTask.CompletedTask;
// //     }
// //     
// //     public virtual async Task Process(TContext context) {
// //         try {
// //             var processedRecords = await ParseRecords(context)
// //                 .OrderBy(record => record.EventTime)
// //                 .SelectAwaitWithCancellation(AddRecord)
// //                 .ToListAsync(context.CancellationToken)
// //                 .ConfigureAwait(false);
// //
// //             foreach (var rec in processedRecords) {
// //                 await rec.EnsureProcessed().ConfigureAwait(false);
// //             }
// //
// //             // await Task.WhenAll(processedRecords.Select(x => x.EnsureProcessed())).ConfigureAwait(false);
// //
// //             await OnSuccessInternal(processedRecords).ConfigureAwait(false);
// //         }
// //         catch (Exception ex) {
// //             await OnErrorInternal(ex).ConfigureAwait(false);
// //         }
// //
// //         async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
// //             if (processedRecords.Any()) {
// //                 var lastEventTime = FromUnixTimeMilliseconds(processedRecords.Last().EventTime);
// //                 
// //                 Log.Information(
// //                     "{RecordCount} record(s) processed up to {Checkpoint:O}",
// //                     processedRecords.Count, lastEventTime
// //                 );
// //             }
// //
// //             try {
// //                 await OnSuccess(context, processedRecords).ConfigureAwait(false);
// //             }
// //             catch (Exception ex) {
// //                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
// //             }
// //         }
// //
// //         async ValueTask OnErrorInternal(Exception exception) {
// //             try {
// //                 await OnError(context, exception).ConfigureAwait(false);
// //             }
// //             catch (Exception ex) {
// //                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
// //             }
// //         }
// //     }
// //
// //     public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);
// //
// //     public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) => ValueTask.CompletedTask;
// //
// //     public virtual ValueTask OnError(TContext context, Exception exception) => ValueTask.CompletedTask;
// // }
//
// [PublicAPI]
// public abstract class DataSourceConnector<TContext> : IDataSource<TContext> where TContext : IDataSourceContext {
//     protected DataSourceConnector() {
//         Name        = GetType().Name;
//         Log         = ForContext(SourceContextPropertyName, Name);
//         Initialized = new();
//     }
//
//     protected InterlockedBoolean Initialized { get; }
//     protected ILogger            Log         { get; }
//     
//     protected KrimsonProducer         Producer    { get; set; } = null!;
//     protected SourceCheckpointManager Checkpoints { get; set; } = null!;
//     protected bool                    Synchronous { get; set; } = true;
//
//     public string Name { get; }
//
//     public bool IsInitialized => Initialized.CurrentValue;
//
//     public virtual IDataSource<TContext> Initialize(IServiceProvider services) {
//         try {
//             if (Initialized.EnsureCalledOnce()) return this;
//
//             Producer    = services.GetRequiredService<KrimsonProducer>();
//             Checkpoints = new(services.GetRequiredService<KrimsonReader>());
//
//             return this;
//         }
//         catch (Exception ex) {
//             throw new Exception($"Failed to initialize connector: {Name}", ex);
//         }
//     }
//
//     // public override Task Process(TContext context) {
//     //     if (Initialized.EnsureCalledOnce()) return base.Process(context);
//     //
//     //     Producer    = context.Services.GetRequiredService<KrimsonProducer>();
//     //     Checkpoints = new(context.Services.GetRequiredService<KrimsonReader>());
//     //
//     //     return base.Process(context);
//     // }
//     
//     // public override async Task Process(TContext context) {
//     //     try {
//     //         var records = ParseRecords(context).OrderBy(record => record.EventTime);
//     //
//     //         await foreach (var record in records.ConfigureAwait(false)) {
//     //             await AddRecord(record, context.CancellationToken).ConfigureAwait(false);
//     //             // await record.EnsureProcessed().ConfigureAwait(false);
//     //         }
//     //
//     //         var processedRecords = await ParseRecords(context)
//     //             .OrderBy(record => record.EventTime)
//     //             .SelectAwaitWithCancellation(AddRecord)
//     //             .ToListAsync(context.CancellationToken)
//     //             .ConfigureAwait(false);
//     //
//     //         foreach (var rec in processedRecords) 
//     //             await rec.EnsureProcessed().ConfigureAwait(false);
//     //
//     //         // await Task.WhenAll(processedRecords.Select(x => x.EnsureProcessed())).ConfigureAwait(false);
//     //
//     //         await OnSuccessInternal(processedRecords).ConfigureAwait(false);
//     //     }
//     //     catch (Exception ex) {
//     //         await OnErrorInternal(ex).ConfigureAwait(false);
//     //     }
//     //
//     //     async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
//     //         if (processedRecords.Any()) {
//     //             var lastEventTime = DateTimeOffset.FromUnixTimeMilliseconds(processedRecords.Last().EventTime);
//     //             
//     //             Log.Information(
//     //                 "{RecordCount} record(s) processed up to {Checkpoint:O}",
//     //                 processedRecords.Count, lastEventTime
//     //             );
//     //         }
//     //
//     //         try {
//     //             await OnSuccess(context, processedRecords).ConfigureAwait(false);
//     //         }
//     //         catch (Exception ex) {
//     //             Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
//     //         }
//     //     }
//     //
//     //     async ValueTask OnErrorInternal(Exception exception) {
//     //         try {
//     //             await OnError(context, exception).ConfigureAwait(false);
//     //         }
//     //         catch (Exception ex) {
//     //             Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
//     //         }
//     //     }
//     // }
//     //
//     
//     public virtual async Task Process(TContext context) {
//         Initialize(context.Services);
//         
//         try {
//             var records = await ParseRecords(context)
//                 .OrderBy(record => record.EventTime)
//                 .SelectAwaitWithCancellation(AddRecord)
//                 .ToListAsync(context.CancellationToken)
//                 .ConfigureAwait(false);
//     
//             // await Flush(records);
//                 
//             await OnSuccessInternal(records).ConfigureAwait(false);
//         }
//         catch (Exception ex) {
//             await OnErrorInternal(ex).ConfigureAwait(false);
//         }
//
//         async Task Flush(IEnumerable<SourceRecord> sourceRecords) {
//             Producer.Flush();
//             //
//             // await Task
//             //     .WhenAll(sourceRecords.Select(x => x.EnsureProcessed()))
//             //     .ConfigureAwait(false);
//         }
//
//         async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
//             try {
//                 if (processedRecords.Any()) {
//                     var skipped          = processedRecords.Where(x => x.ProcessingSkipped).ToList();
//                     var processedByTopic = processedRecords.Where(x => x.ProcessingSuccessful).GroupBy(x => x.DestinationTopic).ToList();
//                 
//                     if (skipped.Any()) {
//                         Log.Information("{RecordCount} record(s) skipped", skipped.Count);
//                     }
//                 
//                     foreach (var recordSet in processedByTopic) {
//                         var lastRecord = recordSet.Last();
//                        
//                         Checkpoints.TrackCheckpoint(SourceCheckpoint.From(lastRecord));
//
//                         var recordCount = recordSet.Count();
//                         
//                         Logger.Debug(
//                             "{SourceName} | {RecordsCount} record(s) processed >> {EventTime} {Topic} [{Partition}] @ {Offset}",
//                             lastRecord.Source, recordCount, FromUnixTimeMilliseconds(lastRecord.EventTime), 
//                             lastRecord.RecordId.Topic, lastRecord.RecordId.Partition, lastRecord.RecordId.Offset
//                         );
//                     }
//                 }
//             }
//             catch (Exception ex) {
//                 throw;
//             }
//
//             try {
//                 await OnSuccess(context, processedRecords).ConfigureAwait(false);
//             }
//             catch (Exception ex) {
//                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
//             }
//         }
//
//         async ValueTask OnErrorInternal(Exception exception) {
//             try {
//                 await OnError(context, exception).ConfigureAwait(false);
//             }
//             catch (Exception ex) {
//                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
//             }
//         }
//     }
//
//     public virtual async ValueTask<SourceRecord> AddRecord(SourceRecord record, CancellationToken cancellationToken) {
//         // ensure source connector name is set
//         record.Source ??= Name;
//         
//         // ensure destination topic is set
//         record.DestinationTopic ??= Producer.Topic;
//
//         if (!record.HasDestinationTopic)
//             throw new($"{Name} found record with missing destination topic!");
//
//         var isUnseen = await IsRecordUnseen().ConfigureAwait(false);
//
//         if (!isUnseen) {
//             record.Skip();
//             return record;
//         }
//         
//         var request = ProducerRequest.Builder
//             .Key(record.Key)
//             .Message(record.Value)
//             .Timestamp(record.EventTime)
//             .Headers(record.Headers)
//             .Topic(record.DestinationTopic)
//             .RequestId(record.RequestId)
//             .Create();
//         
//         if (Synchronous)
//             HandleResult(record, await Producer.Produce(request, throwOnError: false).ConfigureAwait(false));
//         else
//             Producer.Produce(request, result => HandleResult(record, result));
//
//         return record;
//         
//         static void HandleResult(SourceRecord record, ProducerResult result) {
//             if (result.Success)
//                 record.Ack(result.RecordId);
//             else
//                 record.Nak(result.Exception!); //TODO SS: should I trigger the exception right here!?
//         }
//
//         async ValueTask<bool> IsRecordUnseen() {
//             var checkpoint = await Checkpoints
//                 .GetCheckpoint(record.DestinationTopic!, cancellationToken)
//                 .ConfigureAwait(false);
//
//             var unseenRecord = record.EventTime > checkpoint.Timestamp;
//
//             if (!unseenRecord)
//                 Logger.Debug(
//                     "{SourceName} | record already processed at least once | event time: {EventTime} checkpoint: {CheckpointTimestamp}", 
//                     record.Source, record.EventTime, checkpoint.Timestamp
//                 );
//
//             return unseenRecord;
//         }
//     }
//     
//     public abstract IAsyncEnumerable<SourceRecord> ParseRecords(TContext context);
//
//     public virtual ValueTask OnSuccess(TContext context, List<SourceRecord> processedRecords) => ValueTask.CompletedTask;
//
//     public virtual ValueTask OnError(TContext context, Exception exception) => ValueTask.CompletedTask;
//     
//     IAsyncEnumerable<SourceRecord> IDataSource.Records(CancellationToken cancellationToken) {
//         return AsyncEnumerable.Empty<SourceRecord>();
//     }
//     
//     public virtual ValueTask DisposeAsync() {
//         Producer.Flush();
//         return ValueTask.CompletedTask;
//     }
//     
//     //
//     // async ValueTask<SourceRecord> SendSourceRecord(SourceRecord record, bool synchronousDelivery = true) {
//     //     var request = ProducerRequest.Builder
//     //         .Key(record.Key)
//     //         .Message(record.Value)
//     //         .Timestamp(record.EventTime)
//     //         .Headers(record.Headers)
//     //         .Topic(record.DestinationTopic)
//     //         .RequestId(record.RequestId)
//     //         .Create();
//     //
//     //     if (synchronousDelivery)
//     //         HandleResult(record, await Producer.Produce(request, throwOnError: false).ConfigureAwait(false), Checkpoints);
//     //     else
//     //         Producer.Produce(request, result => HandleResult(record, result, Checkpoints));
//     //
//     //     return record;
//     //     
//     //     static void HandleResult(SourceRecord record, ProducerResult result, SourceCheckpointManager checkpoints) {
//     //         if (result.Success) {
//     //             checkpoints.TrackCheckpoint(SourceCheckpoint.From(record));
//     //             
//     //             record.Ack(result.RecordId);
//     //             
//     //             Logger.Debug(
//     //                 "{SourceName} | {RequestId} checkpoint {Topic} [{Partition}] @ {Offset} : {EventTime}",
//     //                 record.Source, result.RequestId, result.RecordId.Topic,
//     //                 result.RecordId.Partition, result.RecordId.Offset, record.EventTime
//     //             );
//     //         }
//     //         else {
//     //             record.Nak(result.Exception!);
//     //         }
//     //     }
//     // }
// }
//
//
// //
// // public abstract class DataSourceConnector<TContext> : DataSource<TContext> where TContext : IDataSourceContext {
// //     KrimsonProducer         Producer    { get; set; } = null!;
// //     SourceCheckpointManager Checkpoints { get; set;}  = null!;
// //     bool                    Synchronous { get; set; } = true;
// //
// //     InterlockedBoolean Initialized { get; } = new InterlockedBoolean();
// //
// //     public IDataSource<TContext> Initialize(IServiceProvider services) {
// //         if (Initialized.EnsureCalledOnce()) return this;
// //
// //         Producer    = services.GetRequiredService<KrimsonProducer>();
// //         Checkpoints = new(services.GetRequiredService<KrimsonReader>());
// //
// //         return this;
// //     }
// //
// //     // public override Task Process(TContext context) {
// //     //     if (Initialized.EnsureCalledOnce()) return base.Process(context);
// //     //
// //     //     Producer    = context.Services.GetRequiredService<KrimsonProducer>();
// //     //     Checkpoints = new(context.Services.GetRequiredService<KrimsonReader>());
// //     //
// //     //     return base.Process(context);
// //     // }
// //     
// //     // public override async Task Process(TContext context) {
// //     //     try {
// //     //         var records = ParseRecords(context).OrderBy(record => record.EventTime);
// //     //
// //     //         await foreach (var record in records.ConfigureAwait(false)) {
// //     //             await AddRecord(record, context.CancellationToken).ConfigureAwait(false);
// //     //             // await record.EnsureProcessed().ConfigureAwait(false);
// //     //         }
// //     //
// //     //         var processedRecords = await ParseRecords(context)
// //     //             .OrderBy(record => record.EventTime)
// //     //             .SelectAwaitWithCancellation(AddRecord)
// //     //             .ToListAsync(context.CancellationToken)
// //     //             .ConfigureAwait(false);
// //     //
// //     //         foreach (var rec in processedRecords) 
// //     //             await rec.EnsureProcessed().ConfigureAwait(false);
// //     //
// //     //         // await Task.WhenAll(processedRecords.Select(x => x.EnsureProcessed())).ConfigureAwait(false);
// //     //
// //     //         await OnSuccessInternal(processedRecords).ConfigureAwait(false);
// //     //     }
// //     //     catch (Exception ex) {
// //     //         await OnErrorInternal(ex).ConfigureAwait(false);
// //     //     }
// //     //
// //     //     async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
// //     //         if (processedRecords.Any()) {
// //     //             var lastEventTime = DateTimeOffset.FromUnixTimeMilliseconds(processedRecords.Last().EventTime);
// //     //             
// //     //             Log.Information(
// //     //                 "{RecordCount} record(s) processed up to {Checkpoint:O}",
// //     //                 processedRecords.Count, lastEventTime
// //     //             );
// //     //         }
// //     //
// //     //         try {
// //     //             await OnSuccess(context, processedRecords).ConfigureAwait(false);
// //     //         }
// //     //         catch (Exception ex) {
// //     //             Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
// //     //         }
// //     //     }
// //     //
// //     //     async ValueTask OnErrorInternal(Exception exception) {
// //     //         try {
// //     //             await OnError(context, exception).ConfigureAwait(false);
// //     //         }
// //     //         catch (Exception ex) {
// //     //             Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
// //     //         }
// //     //     }
// //     // }
// //     //
// //     
// //     public override async Task Process(TContext context) {
// //         try {
// //             var records = await ParseRecords(context)
// //                 .OrderBy(record => record.EventTime)
// //                 .SelectAwaitWithCancellation(AddRecord)
// //                 .ToListAsync(context.CancellationToken)
// //                 .ConfigureAwait(false);
// //             
// //             await Task.WhenAll(records.Select(x => x.EnsureProcessed())).ConfigureAwait(false);
// //                 
// //             await OnSuccessInternal(records).ConfigureAwait(false);
// //         }
// //         catch (Exception ex) {
// //             await OnErrorInternal(ex).ConfigureAwait(false);
// //         }
// //
// //         async ValueTask OnSuccessInternal(List<SourceRecord> processedRecords) {
// //             if (processedRecords.Any()) {
// //                 var skipped          = processedRecords.Where(x => x.ProcessingSkipped).ToList();
// //                 var processedByTopic = processedRecords.Where(x => x.ProcessingSuccessful).GroupBy(x => x.DestinationTopic).ToList();
// //                 
// //                 if (skipped.Any()) {
// //                     Log.Information("{RecordCount} record(s) skipped", skipped.Count);
// //                 }
// //                 
// //                 foreach (var recordSet in processedByTopic) {
// //                     var checkpoint = FromUnixTimeMilliseconds(recordSet.Last().EventTime);
// //                 
// //                     Log.Information(
// //                         "{DestinationTopic} << {RecordCount} record(s) processed up to {Checkpoint:O}",
// //                         recordSet.Key, recordSet.Count(), checkpoint
// //                     );
// //                 }
// //             }
// //
// //             try {
// //                 await OnSuccess(context, processedRecords).ConfigureAwait(false);
// //             }
// //             catch (Exception ex) {
// //                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
// //             }
// //         }
// //
// //         async ValueTask OnErrorInternal(Exception exception) {
// //             try {
// //                 await OnError(context, exception).ConfigureAwait(false);
// //             }
// //             catch (Exception ex) {
// //                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
// //             }
// //         }
// //     }
// //     
// //     async ValueTask<bool> RecordProcessedAtLeastOnce(SourceRecord record, CancellationToken ct) {
// //         var checkpoint = await Checkpoints
// //             .GetCheckpoint(record.DestinationTopic!, ct)
// //             .ConfigureAwait(false);
// //
// //         var processed = record.EventTime >= checkpoint.Timestamp;
// //
// //         if (processed)
// //             Logger.Information(
// //                 "{SourceName} | record received and processed at least once | event time: {EventTime} checkpoint: {CheckpointTimestamp}", 
// //                 record.Source, record.EventTime, checkpoint.Timestamp
// //             );
// //
// //         return processed;
// //     }
// //
// //     public override async ValueTask<SourceRecord> AddRecord(SourceRecord record, CancellationToken cancellationToken) {
// //         // ensure destination topic is set
// //         record.DestinationTopic ??= Producer.Topic;
// //
// //         if (!record.HasDestinationTopic)
// //             throw new("Destination topic is missing!");
// //
// //         var processedAtLeastOnce = await RecordProcessedAtLeastOnce(record, cancellationToken).ConfigureAwait(false);
// //
// //         if (processedAtLeastOnce) {
// //             record.Skip();
// //             return record;
// //         }
// //         
// //         var request = ProducerRequest.Builder
// //             .Key(record.Key)
// //             .Message(record.Value)
// //             .Timestamp(record.EventTime)
// //             .Headers(record.Headers)
// //             .Topic(record.DestinationTopic)
// //             .RequestId(record.RequestId)
// //             .Create();
// //         
// //         if (Synchronous)
// //             HandleResult(record, await Producer.Produce(request, throwOnError: false).ConfigureAwait(false), Checkpoints);
// //         else
// //             Producer.Produce(request, result => HandleResult(record, result, Checkpoints));
// //
// //         return record;
// //         
// //         static void HandleResult(SourceRecord record, ProducerResult result, SourceCheckpointManager checkpoints) {
// //             if (result.Success) {
// //                 checkpoints.TrackCheckpoint(SourceCheckpoint.From(record));
// //                 
// //                 record.Ack(result.RecordId);
// //                 
// //                 Logger.Debug(
// //                     "{SourceName} | {RequestId} checkpoint {Topic} [{Partition}] @ {Offset} : {EventTime}",
// //                     record.Source, result.RequestId, result.RecordId.Topic,
// //                     result.RecordId.Partition, result.RecordId.Offset, record.EventTime
// //                 );
// //             }
// //             else {
// //                 record.Nak(result.Exception!);
// //                 // should I trigger the exception right here!?
// //             }
// //         }
// //     }
// //     
// //     //
// //     // async ValueTask<SourceRecord> SendSourceRecord(SourceRecord record, bool synchronousDelivery = true) {
// //     //     var request = ProducerRequest.Builder
// //     //         .Key(record.Key)
// //     //         .Message(record.Value)
// //     //         .Timestamp(record.EventTime)
// //     //         .Headers(record.Headers)
// //     //         .Topic(record.DestinationTopic)
// //     //         .RequestId(record.RequestId)
// //     //         .Create();
// //     //
// //     //     if (synchronousDelivery)
// //     //         HandleResult(record, await Producer.Produce(request, throwOnError: false).ConfigureAwait(false), Checkpoints);
// //     //     else
// //     //         Producer.Produce(request, result => HandleResult(record, result, Checkpoints));
// //     //
// //     //     return record;
// //     //     
// //     //     static void HandleResult(SourceRecord record, ProducerResult result, SourceCheckpointManager checkpoints) {
// //     //         if (result.Success) {
// //     //             checkpoints.TrackCheckpoint(SourceCheckpoint.From(record));
// //     //             
// //     //             record.Ack(result.RecordId);
// //     //             
// //     //             Logger.Debug(
// //     //                 "{SourceName} | {RequestId} checkpoint {Topic} [{Partition}] @ {Offset} : {EventTime}",
// //     //                 record.Source, result.RequestId, result.RecordId.Topic,
// //     //                 result.RecordId.Partition, result.RecordId.Offset, record.EventTime
// //     //             );
// //     //         }
// //     //         else {
// //     //             record.Nak(result.Exception!);
// //     //         }
// //     //     }
// //     // }
// // }