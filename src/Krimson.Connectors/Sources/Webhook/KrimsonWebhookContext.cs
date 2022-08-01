namespace Krimson.Connectors.Sources;

[PublicAPI]
public class KrimsonWebhookContext : ISourceConnectorContext {
    public KrimsonWebhookContext(HttpContext http) => Http = http;

    public HttpContext Http { get; }
    
    public HttpRequest       Request           => Http.Request;
    public HttpResponse      Response          => Http.Response;
    public CancellationToken CancellationToken => Request.HttpContext.RequestAborted;

    public T GetService<T>() where T : notnull => Http.RequestServices.GetRequiredService<T>();
    
    public async ValueTask SetResult(IResult result) =>
        await result.ExecuteAsync(Http).ConfigureAwait(false);
}