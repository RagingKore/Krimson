// using Krimson.State;
//
// namespace Krimson.Connectors;
//
// [PublicAPI]
// public class KrimsonWebhookContext : ISourceConnectorContext {
//     public KrimsonWebhookContext(HttpContext http) {
//         Http  = http;
//         State = http.RequestServices.GetRequiredService<IStateStore>(); // hello darkness my old friend...
//     }
//
//     public HttpContext Http  { get; }
//     public IStateStore State { get; }
//     
//     public HttpRequest       Request           => Http.Request;
//     public HttpResponse      Response          => Http.Response;
//     public CancellationToken CancellationToken => Request.HttpContext.RequestAborted;
//
//     public T GetService<T>() where T : notnull => Http.RequestServices.GetRequiredService<T>();
//     
//     public async ValueTask SetResult(IResult result) =>
//         await result.ExecuteAsync(Http).ConfigureAwait(false);
// }