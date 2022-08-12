using System.Threading.Channels;
using static System.Threading.Channels.Channel;

namespace Krimson.Connectors;

public record PushDataSourceOptions(int QueueLength = 1000) : IDataSourceOptions;

[PublicAPI]
public abstract class PushDataSource : IDataSource {
    public const int DefaultQueueLength = 1000;

    protected PushDataSource(PushDataSourceOptions? options = null) {
        Channel = CreateBounded<SourceRecord>((options ?? new PushDataSourceOptions()).QueueLength);
    }
    
    Channel<SourceRecord> Channel { get; }

    public virtual async ValueTask<SourceRecord> Push(SourceRecord record, CancellationToken cancellationToken) {
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