using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Krimson.Producers;
using Microsoft.AspNetCore.Mvc;
using static System.String;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;

namespace Krimson.Connectors;

[PublicAPI]
public abstract class KrimsonWebhook : IKrimsonWebhook {
    static readonly JsonSerializerOptions DefaultJsonOptions = new() {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    protected KrimsonWebhook(string? webhookPath = null) {
        WebhookPath = webhookPath ?? GetPathFromAttribute();
        
        if (IsNullOrWhiteSpace(WebhookPath)) 
            throw new ArgumentNullException(nameof(webhookPath), $"Webhook path cannot be empty: {GetType().Name}");
        
        Log = ForContext(SourceContextPropertyName, GetType().Name);

        string GetPathFromAttribute() => 
            (WebhookPathAttribute?)Attribute.GetCustomAttribute(GetType(), typeof(WebhookPathAttribute)) ?? "";
    }
    
    protected ILogger Log { get; }

    public string WebhookPath { get; }

    public async Task Execute(KrimsonWebhookContext context) {
        try {
            var isValid = await Validate(context).ConfigureAwait(false);

            if (!isValid ) {
                if (!context.Response.HasStarted) 
                    await context
                        .SetResult(Results.BadRequest())
                        .ConfigureAwait(false);

                return;
            }

            context.CancellationToken.ThrowIfCancellationRequested();

            var data = SourceData(context);

            var processedRecords = await SourceRecords(data, context.CancellationToken)
                .Where(record => !record.Equals(SourceRecord.Empty))
                .OrderBy(record => record.Timestamp)
                .SelectAwait(DispatchSourceRecord)
                .ToListAsync(context.CancellationToken)
                .ConfigureAwait(false);

            if (processedRecords.Any())
                Log.Information("{RecordCount} record(s) processed", processedRecords.Count);

            await OnSuccess(context, processedRecords).ConfigureAwait(false);

        }
        catch (Exception ex) {
            await OnError(context, ex).ConfigureAwait(false);
        }

        async ValueTask<ProcessedSourceRecord> DispatchSourceRecord(SourceRecord record) {
            var request = ProducerRequest.Builder
                .Key(record.Key)
                .Message(record.Value)
                .Timestamp(record.Timestamp)
                .Topic(record.Topic)
                .Headers(record.Headers)
                .Create();

            var result = await context
                .GetService<KrimsonProducer>()
                .Produce(request)
                .ConfigureAwait(false);
            
            return new ProcessedSourceRecord(record, result.RecordId);
        }
    }

    public virtual async IAsyncEnumerable<JsonNode> SourceData(KrimsonWebhookContext context) {
        var node = await context.Request
            .ReadFromJsonAsync<JsonNode>(DefaultJsonOptions, context.CancellationToken)
            .ConfigureAwait(false);

        if (node is not null)
            yield return node;
    }
    
    public virtual IAsyncEnumerable<SourceRecord> SourceRecords(IAsyncEnumerable<JsonNode> data, CancellationToken cancellationToken) {
        return data.Select(node => {
            try {
                return ParseSourceRecord(node);
            }
            catch (Exception ex) {
                Log.ForContext("JsonData", node.ToJsonString()).Error(ex, "Failed to parse source record!");
                return SourceRecord.Empty;
            }
        });
    }

    public abstract SourceRecord ParseSourceRecord(JsonNode node);
    
    public abstract Task Subscribe(IServiceProvider services, CancellationToken cancellationToken);
    
    public virtual ValueTask<bool> Validate(KrimsonWebhookContext context)  => ValueTask.FromResult(true);

    public virtual ValueTask OnSuccess(KrimsonWebhookContext context, List<ProcessedSourceRecord> processedRecords) => 
        context.SetResult(Results.Ok());

    public virtual ValueTask OnError(KrimsonWebhookContext context, Exception exception) {
        var problem = new ProblemDetails {
            Status   = StatusCodes.Status500InternalServerError,
            Instance = context.Request.Path,
            Title    = exception.Message,
            Detail   = exception.StackTrace
        };

        return context.SetResult(Results.Problem(problem));
    }
}

//
// [PublicAPI]
// public abstract class KrimsonWebhookReturnsResult: IKrimsonWebhook<JsonNode> {
//     static readonly JsonSerializerOptions DefaultJsonOptions = new() {
//         DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
//     };
//     
//     protected KrimsonWebhookReturnsResult() => Log = ForContext(SourceContextPropertyName, GetType().Name);
//
//     protected ILogger Log { get; }
//     
//     public abstract string WebhookPath { get; }
//     
//     public async Task<IResult> Execute(WebhookContext context) {
//         var validationResult = Validate(context);
//
//         if (validationResult.Value is false)
//             return Results.BadRequest();
//
//         if (validationResult.Value is IResult errorResult)
//             return errorResult;
//         
//         try {
//             var data = await context.Request
//                 .ReadFromJsonAsync<JsonNode>(DefaultJsonOptions, context.CancellationToken)
//                 .ConfigureAwait(false);
//
//             var dispatched = await SourceRecords(data, context.CancellationToken)
//                 .Where(record => !record.Equals(SourceRecord.Empty))
//                 .OrderBy(record => record.Timestamp)
//                 .SelectAwait(async record => {
//                     var result = await context.Producer.Produce(record, record.Id).ConfigureAwait(false);
//                     return (record, result.RecordId);
//                 })
//                 .ToListAsync(context.CancellationToken)
//                 .ConfigureAwait(false);
//
//
//             if (dispatched.Any())
//                 Log.Information("{RecordCount} record(s) dispatched", dispatched.Count);
//
//             return OnSuccess(context, dispatched);
//         }
//         catch (Exception ex) {
//             return OnError(context, ex);
//         }
//     }
//     
//     public async Task Execute2(WebhookContext context) {
//         var validationResult = Validate(context);
//
//         if (validationResult.Value is false) {
//             await Results.BadRequest().ExecuteAsync(context.Request.HttpContext).ConfigureAwait(false);
//             return;
//         }
//
//         if (validationResult.Value is IResult errorResult) {
//             await errorResult.ExecuteAsync(context.Request.HttpContext).ConfigureAwait(false);
//             return;
//         }
//
//         try {
//             var data = await context.Request
//                 .ReadFromJsonAsync<JsonNode>(DefaultJsonOptions, context.CancellationToken)
//                 .ConfigureAwait(false);
//
//             var dispatched = await SourceRecords(data, context.CancellationToken)
//                 .Where(record => !record.Equals(SourceRecord.Empty))
//                 .OrderBy(record => record.Timestamp)
//                 .SelectAwait(async record => {
//                     var result = await context.Producer.Produce(record, record.Id).ConfigureAwait(false);
//                     return (record, result.RecordId);
//                 })
//                 .ToListAsync(context.CancellationToken)
//                 .ConfigureAwait(false);
//
//
//             if (dispatched.Any())
//                 Log.Information("{RecordCount} record(s) dispatched", dispatched.Count);
//
//             await OnSuccess(context, dispatched).ExecuteAsync(context.Request.HttpContext).ConfigureAwait(false);
//         }
//         catch (Exception ex) {
//             await OnError(context, ex).ExecuteAsync(context.Request.HttpContext).ConfigureAwait(false);
//         }
//     }
//     
//     public abstract IAsyncEnumerable<SourceRecord> SourceRecords(JsonNode? data, CancellationToken cancellationToken);
//     
//     public virtual OneOf<bool, IResult> Validate(WebhookContext context) => true;
//     
//     public virtual IResult OnSuccess(WebhookContext context, List<(SourceRecord SourceRecord, RecordId RecordId)> processed) =>
//         Results.Ok();
//
//     public virtual IResult OnError(WebhookContext context, Exception exception) =>
//         Results.Problem(
//             new ProblemDetails {
//                 Status   = StatusCodes.Status500InternalServerError,
//                 Instance = context.Request.Path,
//                 Title    = exception.Message,
//                 Detail   = exception.StackTrace
//             }
//         );
// }