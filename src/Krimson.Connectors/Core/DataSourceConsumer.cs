using Krimson.Connectors.Checkpoints;
using Krimson.Producers;

namespace Krimson.Connectors;

public class DataSourceConsumer<TSource> : BackgroundService where TSource : IDataSource {
    public DataSourceConsumer(TSource source, KrimsonProducer producer) {
        Source   = source;
        Producer = producer;
    }

    TSource         Source   { get; }
    KrimsonProducer Producer { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        await foreach(var record in Source.Records(stoppingToken).ConfigureAwait(false))
            await Producer
                .PushSourceRecord(record)
                .ConfigureAwait(false);
    }
}

public class DataSourceConsumer : BackgroundService {
    public DataSourceConsumer(IEnumerable<IDataSource> sources, KrimsonProducer producer) {
        Sources  = sources;
        Producer = producer;
    }

    IEnumerable<IDataSource> Sources  { get; }
    KrimsonProducer          Producer { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var records = Sources.ToAsyncEnumerable().SelectMany(x => x.Records(stoppingToken)).ConfigureAwait(false);
        
        await foreach(var record in records)
            await Producer
                .PushSourceRecord(record)
                .ConfigureAwait(false);
    }
}