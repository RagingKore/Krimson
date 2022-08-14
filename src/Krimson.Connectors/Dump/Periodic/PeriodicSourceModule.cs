// namespace Krimson.Connectors;
//
// [PublicAPI]
// public abstract class PeriodicSourceModule : IDataSourceModule<PeriodicSourceContext>  {
//     protected PeriodicSourceModule(TimeSpan? backoffTime = null) {
//         BackoffTime = backoffTime ?? GetBackoffTimeFromAttribute();
//
//         TimeSpan GetBackoffTimeFromAttribute() => 
//             (BackOffTimeAttribute?)Attribute.GetCustomAttribute(GetType(), typeof(BackOffTimeAttribute)) ?? TimeSpan.FromSeconds(30);
//     }
//     
//     public TimeSpan BackoffTime { get; }
//
//     public virtual ValueTask<bool> OnValidate(PeriodicSourceContext context) => 
//         ValueTask.FromResult(true);
//
//     public abstract IAsyncEnumerable<SourceRecord> ParseRecords(PeriodicSourceContext context);
//
//     public virtual ValueTask OnSuccess(PeriodicSourceContext context, List<SourceRecord> processedRecords) =>
//         ValueTask.CompletedTask;
//
//     public ValueTask OnError(PeriodicSourceContext context, Exception exception) =>
//         ValueTask.CompletedTask;
// }