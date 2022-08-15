using Krimson.Connectors.Checkpoints;
using Krimson.Producers;
using Krimson.Readers;
using Serilog;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

public class DataSourceConsumer<TSource> : DataSourceConsumer where TSource : IDataSource {
    public DataSourceConsumer(TSource source, KrimsonProducer producer, KrimsonReader reader) 
        : base(new IDataSource[]{ source }, producer, reader) { }
}

public class DataSourceConsumer : BackgroundService {
    static readonly ILogger Logger = Log.ForContext<DataSourceConsumer>();
    
    public DataSourceConsumer(IEnumerable<IDataSource> sources, KrimsonProducer producer, KrimsonReader reader) {
        Sources     = sources;
        Producer    = producer;
        Checkpoints = new(reader);
    }

    IEnumerable<IDataSource> Sources     { get; }
    KrimsonProducer          Producer    { get; }
    SourceCheckpointManager  Checkpoints { get; }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var records = Sources.ToAsyncEnumerable()
            .SelectMany(x => x.Records(stoppingToken))
            .WhereAwaitWithCancellation(RecordIsUnseen)
            .ConfigureAwait(false);

        await foreach (var record in records) {
            await Producer
                .SendSourceRecord(record)
                .ConfigureAwait(false);
        }
        
        async ValueTask<bool> RecordIsUnseen(SourceRecord record, CancellationToken ct) {
            var checkpoint = await Checkpoints
                .GetCheckpoint(record.DestinationTopic!, ct)
                .ConfigureAwait(false);

            return record.EventTime > checkpoint.Timestamp;
        }
    }
}