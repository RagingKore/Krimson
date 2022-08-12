using Microsoft.AspNetCore.Mvc;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors.Http;

// [PublicAPI]
// public class WebhookSource : IExecutableDataSource<WebhookSourceModule, WebhookSourceContext> {
//     static readonly ILogger Log = ForContext<WebhookSource>();
//
//     public WebhookSource(WebhookSourceOptions options) => Options = options;
//
//     WebhookSourceOptions Options { get; }
//     
//     public async Task Execute(WebhookSourceModule module, WebhookSourceContext context) {
//         try {
//             var isValid = await module.OnValidate(context).ConfigureAwait(false);
//
//             if (!isValid ) {
//                 // return bad request by default
//                 if (!context.Response.HasStarted) 
//                     await context
//                         .SetResult(Results.BadRequest())
//                         .ConfigureAwait(false);
//
//                 // get out
//                 return;
//             }
//
//             context.CancellationToken.ThrowIfCancellationRequested();
//             
//             var processedRecords = await module.ParseRecords(context)
//                 .Where(record => record != SourceRecord.Empty)
//                 .OrderBy(record => record.EventTime)
//                 .SelectAwaitWithCancellation(Push)
//                 .ToListAsync(context.CancellationToken)
//                 .ConfigureAwait(false);
//
//             if (processedRecords.Any())
//                 Log.Information("{RecordCount} record(s) processed", processedRecords.Count);
//
//             await OnSuccess(processedRecords).ConfigureAwait(false);
//         }
//         catch (Exception ex) {
//             await OnError(ex).ConfigureAwait(false);
//         }
//
//         async ValueTask OnSuccess(List<SourceRecord> processedRecords) {
//             if (processedRecords.Any()) {
//                 Log.Information("{RecordCount} record(s) processed", processedRecords.Count);
//             }
//
//             try {
//                 await module
//                     .OnSuccess(context, processedRecords)
//                     .ConfigureAwait(false);
//             }
//             catch (Exception ex) {
//                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnSuccess), ex.Message);
//             }
//         }
//         
//         async ValueTask OnError(Exception exception) {
//             try {
//                 await module
//                     .OnError(context, exception)
//                     .ConfigureAwait(false);
//             }
//             catch (Exception ex) {
//                 Log.Error("{Event} user exception: {ErrorMessage}", nameof(OnError), ex.Message);
//             }
//         }
//     }
//
//     public async ValueTask<SourceRecord> Push(SourceRecord record, CancellationToken cancellationToken) {
//         await Options.Producer
//             .PushSourceRecord(record)
//             .ConfigureAwait(false);
//             
//         await record
//             .EnsureProcessed()
//             .ConfigureAwait(false);
//
//         return record;
//     }
//
//     public IAsyncEnumerable<SourceRecord> Records(CancellationToken cancellationToken) => 
//         AsyncEnumerable.Empty<SourceRecord>();
//
//     public ValueTask DisposeAsync() => 
//         ValueTask.CompletedTask;
// }

public abstract class WebhookSource : AutonomousPushSource<WebhookSourceContext> {
    protected WebhookSource() {
        WebhookPath = (WebhookPathAttribute?)Attribute.GetCustomAttribute(GetType(), typeof(WebhookPathAttribute)) ?? "";
    }

    public string WebhookPath { get; }
    
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

    public virtual ValueTask<bool> OnValidate(WebhookSourceContext context) => ValueTask.FromResult(true);

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