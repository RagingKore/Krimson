using Google.Protobuf.WellKnownTypes;
using Krimson.Producers;

namespace Krimson.Connectors;

[PublicAPI] 
interface IPushSourceConnector {
    ValueTask<Timestamp>           LoadCheckpoint(CancellationToken cancellationToken);
    IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken);
    ValueTask<bool>                FilterRecord(SourceRecord record, Timestamp checkpoint, CancellationToken cancellationToken);
    ValueTask<ProducerResult>      DispatchRecord(KrimsonProducer producer, SourceRecord record, CancellationToken cancellationToken);
}