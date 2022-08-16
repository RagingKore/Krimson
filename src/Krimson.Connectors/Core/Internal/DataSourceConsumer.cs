// using Krimson.Connectors.Checkpoints;
// using Krimson.Producers;
// using Krimson.Readers;
// using Serilog;
// using ILogger = Serilog.ILogger;
//
// namespace Krimson.Connectors;
//
// public class DataSourceConsumer<TSource> : DataSourceConsumer where TSource : IDataSource {
//     public DataSourceConsumer(TSource source, KrimsonProducer producer, KrimsonReader reader) 
//         : base(new IDataSource[]{ source }, producer, reader) { }
// }
//
// public class DataSourceConsumer : BackgroundService {
//     static readonly ILogger Logger = Log.ForContext<DataSourceConsumer>();
//     
//     public DataSourceConsumer(IEnumerable<IDataSource> sources, KrimsonProducer producer, KrimsonReader reader) {
//         Sources     = sources;
//         Producer    = producer;
//         Checkpoints = new(reader);
//     }
//
//     IEnumerable<IDataSource> Sources     { get; }
//     KrimsonProducer          Producer    { get; }
//     SourceCheckpointManager  Checkpoints { get; }
//     
//     protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
//         var records = Sources.ToAsyncEnumerable()
//             .SelectMany(x => x.Records(stoppingToken))
//             .Select(EnsureDestinationTopicIsSet)
//             .WhereAwaitWithCancellation(RecordIsUnseen);
//
//         await foreach (var record in records.WithCancellation(stoppingToken).ConfigureAwait(false)) {
//             await SendSourceRecord(record).ConfigureAwait(false);
//         }
//         
//         Logger.Information("Done? Should be eternal!!!!");
//         
//         SourceRecord EnsureDestinationTopicIsSet(SourceRecord record) {
//             record.DestinationTopic ??= Producer.Topic;
//             return record;
//         }
//         
//         async ValueTask<bool> RecordIsUnseen(SourceRecord record, CancellationToken ct) {
//             var checkpoint = await Checkpoints
//                 .GetCheckpoint(record.DestinationTopic!, ct)
//                 .ConfigureAwait(false);
//
//             var unseen = record.EventTime > checkpoint.Timestamp;
//
//             if (!unseen) {
//                 Logger.Information("Unseen: {RecordKey} {Unseen} {EventTime} Checkpoint: {CheckpointTimestamp}", record.Key, unseen, record.EventTime, checkpoint.Timestamp);
//                 record.Skip();
//                 // Checkpoints.TrackCheckpoint(new(record.RecordId, record.EventTime));
//             }
//             
//             return unseen;
//         }
//     }
//     async ValueTask<SourceRecord> SendSourceRecord(SourceRecord record, bool synchronousDelivery = false) {
//         var request = ProducerRequest.Builder
//             .Key(record.Key)
//             .Message(record.Value)
//             .Timestamp(record.EventTime)
//             .Headers(record.Headers)
//             .Topic(record.DestinationTopic)
//             .Create();
//
//         if (synchronousDelivery) {
//             var result = await Producer.Produce(request, throwOnError: false).ConfigureAwait(false);
//
//             HandleResult(record, result, Checkpoints);
//         }
//         else
//             Producer.Produce(request, result => HandleResult(record, result, Checkpoints));
//
//         return record;
//         
//         static void HandleResult(SourceRecord record, ProducerResult result, SourceCheckpointManager checkpoints) {
//             if (result.Success) {
//                 record.Ack(result.RecordId);
//                 
//                 //checkpoints.TrackCheckpoint(SourceCheckpoint.From(record));
//                 
//                 checkpoints.TrackCheckpoint(new(result.RecordId, result.Timestamp.ToUnixTimeMilliseconds()));
//             }
//             else {
//                 Logger.Fatal(result.Exception, "NAK");
//                 record.Nak(result.Exception!);
//             }
//         }
//     }
//
//     public override void Dispose() {
//         Producer.Flush();
//         base.Dispose();
//     }
// }