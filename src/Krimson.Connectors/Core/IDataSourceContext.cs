using Krimson.State;

namespace Krimson.Connectors;

public interface IDataSourceContext {
    IStateStore             State             { get; }
    CancellationTokenSource Cancellator       { get; }
    CancellationToken       CancellationToken { get; }

    internal IServiceProvider   Services { get; set; }
    internal Counter            Counter  { get; set; }
    internal List<SourceRecord> Records  { get; set; }

    IAsyncEnumerable<SourceRecord> ProcessedRecords => Records.ToAsyncEnumerable();

    public void TrackRecord(SourceRecord record) {
        if (record != SourceRecord.Empty) {
            Records.Add(record);
        }
    }
}