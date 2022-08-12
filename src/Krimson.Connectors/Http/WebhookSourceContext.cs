using Krimson.Connectors.Checkpoints;
using Krimson.State;

namespace Krimson.Connectors.Http;

// [PublicAPI]
// public class WebhookSourceContext : IDataSourceContext {
//     public WebhookSourceContext(HttpContext http) {
//         Http       = http;
//         State      = http.RequestServices.GetRequiredService<IStateStore>();
//         Checkpoint = SourceCheckpoint.None;
//         Services   = http.RequestServices;
//     }
//
//     public IServiceProvider Services   { get; }
//     public IStateStore      State      { get; }
//     public SourceCheckpoint Checkpoint { get; private set; }
//     public HttpContext      Http       { get; }
//
//     public HttpRequest       Request           => Http.Request;
//     public HttpResponse      Response          => Http.Response;
//     public CancellationToken CancellationToken => Http.Request.HttpContext.RequestAborted;
//
//     public void SetCheckpoint(SourceCheckpoint checkpoint) => Checkpoint = checkpoint;
//
//     public async ValueTask SetResult(IResult result) => 
//         await result.ExecuteAsync(Http).ConfigureAwait(false);
// }

[PublicAPI]
public class WebhookSourceContext : PushSourceContext {
    public WebhookSourceContext(HttpContext http) : base(http.RequestServices, http.RequestAborted) {
        Http = http;
    }

    public HttpContext Http { get; }

    public HttpRequest  Request  => Http.Request;
    public HttpResponse Response => Http.Response;

    public async ValueTask SetResult(IResult result) => await result.ExecuteAsync(Http).ConfigureAwait(false);
}