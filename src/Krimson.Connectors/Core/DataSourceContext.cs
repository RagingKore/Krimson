using Krimson.Persistence.State;
using static System.Threading.CancellationTokenSource;

namespace Krimson.Connectors;

[PublicAPI]
public class DataSourceContext : IDataSourceContext {
    public DataSourceContext(IServiceProvider services, CancellationToken cancellationToken) {
        this.As<IDataSourceContext>().Services = services;
        this.As<IDataSourceContext>().Counter  = new();
        this.As<IDataSourceContext>().Records  = new();

        State             = this.As<IDataSourceContext>().Services.GetService<IStateStore>() ?? new InMemoryStateStore();
        Cancellator       = CreateLinkedTokenSource(cancellationToken);
        CancellationToken = Cancellator.Token;
    }

    IServiceProvider   IDataSourceContext.Services { get; set; }
    Counter            IDataSourceContext.Counter  { get; set; }
    List<SourceRecord> IDataSourceContext.Records  { get; set; }

    public CancellationTokenSource     Cancellator       { get; }
    public IStateStore                 State             { get; }
    public CancellationToken           CancellationToken { get; }

    public IAsyncEnumerable<SourceRecord> ProcessedRecords => this.As<IDataSourceContext>().Records.ToAsyncEnumerable();
    public IAsyncEnumerable<SourceRecord> SkippedRecords   => ProcessedRecords.Where(record => record.ProcessingSkipped);
}