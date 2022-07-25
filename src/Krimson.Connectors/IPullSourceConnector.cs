using Google.Protobuf.WellKnownTypes;
using Krimson.Producers;

namespace Krimson.Connectors;

[PublicAPI]
public interface IPullSourceConnector {
    Task                           Execute(CancellationToken stoppingToken);
    IAsyncEnumerable<SourceRecord> LoadState(CancellationToken cancellationToken);
    ValueTask<Timestamp>           LoadCheckpoint(CancellationToken cancellationToken);
    IAsyncEnumerable<SourceRecord> PullRecords(CancellationToken cancellationToken);
    ValueTask<bool>                FilterRecord(SourceRecord record, Timestamp checkpoint, CancellationToken cancellationToken);
    ValueTask<ProducerResult>      DispatchRecord(KrimsonProducer producer, SourceRecord record, CancellationToken cancellationToken);
}