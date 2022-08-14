namespace Krimson.Connectors.Http;

[PublicAPI]
public class WebhookSourceContext : DataSourceContext {
    public WebhookSourceContext(HttpContext http) : base(http.RequestServices, http.RequestAborted) {
        Http = http;
    }

    public HttpContext Http { get; }

    public HttpRequest  Request  => Http.Request;
    public HttpResponse Response => Http.Response;

    public async ValueTask SetResult(IResult result) =>
        await result.ExecuteAsync(Http).ConfigureAwait(false);
}