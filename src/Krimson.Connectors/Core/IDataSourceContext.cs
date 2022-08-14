using Krimson.State;

namespace Krimson.Connectors;

public interface IDataSourceContext {
    IServiceProvider  Services          { get; }
    IStateStore       State             { get; }
    CancellationToken CancellationToken { get; }
}