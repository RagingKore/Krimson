using Krimson.State;
using static System.Threading.CancellationTokenSource;

namespace Krimson.Connectors;

[PublicAPI]
public class DataSourceContext : IDataSourceContext {
    public DataSourceContext(IServiceProvider services, CancellationToken cancellationToken) {
        this.As<IDataSourceContext>().Services    = services;
        this.As<IDataSourceContext>().Counter     = new();
        this.As<IDataSourceContext>().Cancellator = CreateLinkedTokenSource(cancellationToken);
        
        State             = this.As<IDataSourceContext>().Services.GetService<IStateStore>() ?? new InMemoryStateStore();
        CancellationToken = this.As<IDataSourceContext>().Cancellator.Token;
    }

    IServiceProvider        IDataSourceContext.Services    { get; set; }
    Counter                 IDataSourceContext.Counter     { get; set; }
    CancellationTokenSource IDataSourceContext.Cancellator { get; set; }
    
    public IStateStore       State             { get; }
    public CancellationToken CancellationToken { get; }
}