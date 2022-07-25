// using Krimson.Producers;
// using Krimson.Readers;
// using static Serilog.Core.Constants;
// using static Serilog.Log;
// using ILogger = Serilog.ILogger;
// using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;
//
// namespace Krimson.Connectors;
//
// [PublicAPI]
// public abstract class PushSourceConnector : IPushSourceConnector {
//     static readonly Timestamp DefaultCheckpoint = Timestamp.FromDateTimeOffset(DateTimeOffset.MinValue);
//
//     protected PushSourceConnector(KrimsonProducer producer, KrimsonReader reader) {
//         Producer = producer;
//         Reader   = reader;
//
//         Log        = ForContext(SourceContextPropertyName, GetType().Name);
//         State      = new Dictionary<string, SourceRecord>();
//         Checkpoint = DefaultCheckpoint;
//         IsFirstRun = new InterlockedBoolean(true);
//         IsBusy     = new InterlockedBoolean();
//     }
//
//     protected ILogger Log { get; }
//
//     KrimsonProducer                  Producer { get; }
//     KrimsonReader                    Reader   { get; }
//     Dictionary<string, SourceRecord> State    { get; }
//
//     Timestamp          Checkpoint { get; set; }
//     InterlockedBoolean IsFirstRun { get; set; }
//     InterlockedBoolean IsBusy     { get; set; }
//
//     public virtual async Task Execute(CancellationToken stoppingToken) {
//        
//     }
//
//     public virtual IAsyncEnumerable<SourceRecord> LoadState(CancellationToken cancellationToken = default) => AsyncEnumerable.Empty<SourceRecord>();
//
//     public virtual async ValueTask<Timestamp> LoadCheckpoint(CancellationToken cancellationToken = default) {
//         if (Producer.Topic is null)
//             return DefaultCheckpoint;
//
//         var checkpoint = await Reader
//             .LastRecords(Producer.Topic!, cancellationToken)
//             .Select(x => ((SourceRecord)x.Value).Timestamp)
//             .FirstOrDefaultAsync(cancellationToken);
//
//         return checkpoint ?? DefaultCheckpoint;
//     }
//     
//     public abstract IAsyncEnumerable<SourceRecord> PushRecords(CancellationToken cancellationToken = default);
//
//     public virtual ValueTask<bool> FilterRecord(SourceRecord record, Timestamp checkpoint, CancellationToken cancellationToken = default) {
//         return ValueTask.FromResult(record.Timestamp > checkpoint);
//     }
//
//     public virtual async ValueTask<ProducerResult>
//         DispatchRecord(KrimsonProducer producer, SourceRecord record, CancellationToken cancellationToken = default) =>
//         await producer.Produce(record, record.Id).ConfigureAwait(false);
// }