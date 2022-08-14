using System.Threading.Channels;
using static System.Threading.Channels.Channel;

namespace Krimson.Connectors;

public record DataSourceOptions(int QueueLength = 1000);

[PublicAPI]
public abstract class DataSource : IDataSource {
    public const int DefaultQueueLength = 1000;

    protected DataSource(DataSourceOptions? options = null) {
        Channel = CreateBounded<SourceRecord>((options ?? new DataSourceOptions()).QueueLength);
    }
    
    Channel<SourceRecord> Channel { get; }

    public virtual async ValueTask<SourceRecord> AddRecord(SourceRecord record, CancellationToken cancellationToken) {
        await Channel.Writer
            .WriteAsync(record, cancellationToken)
            .ConfigureAwait(false);
        
        return record;
    }
    
    public virtual IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken) => 
        Channel.Reader.ReadAllAsync(cancellationToken);
    
    public virtual ValueTask DisposeAsync() {
        Channel.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}