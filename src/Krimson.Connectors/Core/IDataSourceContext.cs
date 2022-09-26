using Krimson.State;

namespace Krimson.Connectors;

public interface IDataSourceContext {
    IStateStore       State             { get; }
    CancellationToken CancellationToken { get; }

    internal IServiceProvider        Services    { get; set; }
    internal Counter                 Counter     { get; set; }
    internal CancellationTokenSource Cancellator { get; set; }
}