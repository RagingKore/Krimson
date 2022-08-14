using Microsoft.AspNetCore.Mvc;

namespace Krimson.Connectors.Http;

[PublicAPI]
public abstract class WebhookSourceConnector : ExecutableDataSource<WebhookSourceContext> {
    public override async Task Execute(WebhookSourceContext context) {
        var isValid = await OnValidate(context).ConfigureAwait(false);

        if (!isValid ) {
            // return bad request by default
            if (!context.Response.HasStarted) 
                await context
                    .SetResult(Results.BadRequest())
                    .ConfigureAwait(false);

            // get out
            return;
        }

        await base.Execute(context).ConfigureAwait(false);
    }

    public virtual ValueTask<bool> OnValidate(WebhookSourceContext context) =>
        ValueTask.FromResult(true);

    public new virtual ValueTask OnSuccess(WebhookSourceContext context, List<SourceRecord> processedRecords) => 
        context.SetResult(Results.Accepted());

    public new virtual ValueTask OnError(WebhookSourceContext context, Exception exception) {
        var problem = new ProblemDetails {
            Status   = StatusCodes.Status500InternalServerError,
            Instance = context.Request.Path,
            Title    = exception.Message,
            Detail   = exception.StackTrace
        };

        return context.SetResult(Results.Problem(problem));
    }
} 