// using Krimson.Connectors.Checkpoints;
// using Krimson.State;
//
// namespace Krimson.Connectors;
//
// [PublicAPI]
// public class PeriodicSourceContext : IDataSourceContext {
//     internal PeriodicSourceContext(IStateStore stateStore, CancellationToken cancellationToken) {
//         State             = stateStore;
//         CancellationToken = cancellationToken;
//         Checkpoint        = SourceCheckpoint.None;
//     }
//
//     public IStateStore       State             { get; }
//     public CancellationToken CancellationToken { get; }
//     public SourceCheckpoint  Checkpoint        { get; internal set; }
// }