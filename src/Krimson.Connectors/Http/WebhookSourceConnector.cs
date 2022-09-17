using Microsoft.AspNetCore.Mvc;

namespace Krimson.Connectors.Http;

public delegate ValueTask<bool> OnValidate(WebhookSourceContext context);

[PublicAPI]
public abstract class WebhookSourceConnector : DataSourceConnector<WebhookSourceContext> {
    protected WebhookSourceConnector() {
        OnValidateHandler = _ => ValueTask.FromResult(true);
        
        OnSuccess(ctx => ctx.SetResult(Results.Accepted()));
        
        OnError((ctx, ex) => {
            var problem = new ProblemDetails {
                Status   = StatusCodes.Status500InternalServerError,
                Instance = ctx.Request.Path,
                Title    = ex.Message,
                Detail   = ex.StackTrace
            };

            return ctx.SetResult(Results.Problem(problem));
        });
    }
    
    OnValidate OnValidateHandler { get; set; }

    public override async Task Process(WebhookSourceContext context) {
        try {
            var isValid = await OnValidateHandler(context).ConfigureAwait(false);

            if (!isValid ) {
                Log.Error("Invalid request on {RequestPath}!", context.Request.Path);
            
                // return bad request by default
                if (!context.Response.HasStarted) 
                    await context
                        .SetResult(Results.BadRequest())
                        .ConfigureAwait(false);

                // get out
                return;
            }
        }
        catch (Exception ex) {
            Log.Error("{Event} User exception: {ErrorMessage}", nameof(OnValidate), ex.Message);
            throw;
        }

        await base.Process(context).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Executed when receiving a request before processing any data.
    /// </summary>
    protected void OnValidate(OnValidate handler) => 
        OnValidateHandler = handler ?? throw new ArgumentNullException(nameof(handler));
} 