using Microsoft.AspNetCore.Mvc;
using static System.String;

namespace Krimson.Connectors.Http;

// [PublicAPI]
// public abstract class WebhookSourceModule : IDataSourceModule<WebhookSourceContext>  {
//     protected WebhookSourceModule(string? webhookPath = null) {
//         WebhookPath = webhookPath ?? GetPathFromAttribute();
//         
//         if (IsNullOrWhiteSpace(WebhookPath)) 
//             throw new ArgumentNullException(nameof(webhookPath), $"Webhook path cannot be empty: {GetType().Name}");
//
//         string GetPathFromAttribute() => 
//             (WebhookPathAttribute?)Attribute.GetCustomAttribute(GetType(), typeof(WebhookPathAttribute)) ?? "";
//     }
//     
//     public string WebhookPath { get; }
//
//     public virtual ValueTask<bool> OnValidate(WebhookSourceContext context) => 
//         ValueTask.FromResult(true);
//
//     public abstract IAsyncEnumerable<SourceRecord> ParseRecords(WebhookSourceContext context);
//
//     public virtual ValueTask OnSuccess(WebhookSourceContext context, List<SourceRecord> processedRecords) => 
//         context.SetResult(Results.Accepted());
//
//     public virtual ValueTask OnError(WebhookSourceContext context, Exception exception) {
//         var problem = new ProblemDetails {
//             Status   = StatusCodes.Status500InternalServerError,
//             Instance = context.Request.Path,
//             Title    = exception.Message,
//             Detail   = exception.StackTrace
//         };
//
//         return context.SetResult(Results.Problem(problem));
//     }
// }

// public delegate ValueTask<bool> OnValidation(WebhookSourceContext context);
// public delegate ValueTask       OnProcessingSuccess(WebhookSourceContext context, List<SourceRecord> processedRecords);
// public delegate ValueTask       OnProcessingError(WebhookSourceContext context, Exception exception);
//
// [PublicAPI]
// public abstract class WebhookSourceModule2  {
//     protected WebhookSourceModule2(string? webhookPath = null) {
//         WebhookPath = webhookPath ?? GetPathFromAttribute();
//         
//         if (String.IsNullOrWhiteSpace(WebhookPath)) 
//             throw new ArgumentNullException(nameof(webhookPath), $"Webhook path cannot be empty: {GetType().Name}");
//
//         string GetPathFromAttribute() => 
//             (WebhookPathAttribute?)Attribute.GetCustomAttribute(GetType(), typeof(WebhookPathAttribute)) ?? "";
//
//         OnValidate = context => ValueTask.FromResult(true); 
//         
//         OnSuccess = (context, records) => context.SetResult(Results.Accepted());
//         
//         OnError = (context, exception) => {
//             var problem = new ProblemDetails {
//                 Status   = StatusCodes.Status500InternalServerError,
//                 Instance = context.Request.Path,
//                 Title    = exception.Message,
//                 Detail   = exception.StackTrace
//             };
//
//             return context.SetResult(Results.Problem(problem));
//         };
//     }
//     
//     public string WebhookPath { get; }
//
//     internal OnValidation        OnValidate { get; set; }
//     internal OnProcessingSuccess OnSuccess  { get; set; }
//     internal OnProcessingError   OnError    { get; set; }
//
//     public abstract IAsyncEnumerable<SourceRecord> ParseRecords(WebhookSourceContext context);
//
//     protected ValueTask<bool> OnValidate(Validate handler) {
//         Validate = handler;
//     }
//
//     protected void OnSuccess(OnSuccess handler);
//     
//     protected void OnError(OnError handler);
// }