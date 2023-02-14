using System.Collections.Concurrent;
using Krimson.Persistence.State;
using static System.Threading.CancellationTokenSource;

namespace Krimson.Connectors;

[PublicAPI]
public class DataSourceContext : IDataSourceContext {
    public DataSourceContext(IServiceProvider services, CancellationToken cancellationToken) {
        this.As<IDataSourceContext>().Counter  = new();
        this.As<IDataSourceContext>().Records  = new();

        Services          = services;
        State             = Services.GetService<IStateStore>() ?? new InMemoryStateStore();
        Cancellator       = CreateLinkedTokenSource(cancellationToken);
        CancellationToken = Cancellator.Token;
    }


    Counter                       IDataSourceContext.Counter  { get; set; }
    ConcurrentQueue<SourceRecord> IDataSourceContext.Records  { get; set; }

    public IServiceProvider        Services          { get; }
    public CancellationTokenSource Cancellator       { get; }
    public IStateStore             State             { get; }
    public CancellationToken       CancellationToken { get; }

    public IAsyncEnumerable<SourceRecord> ProcessedRecords => this.As<IDataSourceContext>().Records.ToAsyncEnumerable();
    public IAsyncEnumerable<SourceRecord> SkippedRecords   => ProcessedRecords.Where(record => record.ProcessingSkipped);
}