// using System.Collections.Concurrent;
// using System.Diagnostics;
// using Krimson.Interceptors;
// using Krimson.Processors.Interceptors;
// using Krimson.Producers.Interceptors;
// using OpenTelemetry.Trace;
//
// namespace Krimson.OpenTelemetry;
//
// [PublicAPI]
// class OpenTelemetryProcessorInterceptor : InterceptorModule {
//     public OpenTelemetryProcessorInterceptor(string sourceName) {
//         InputActivities  = new ConcurrentDictionary<RecordId, Activity>();
//         OutputActivities = new ConcurrentDictionary<Guid, Activity>();
//         ActivitySource   = new ActivitySource(sourceName);
//         
//         On<InputReady>(
//             evt => {
//                 var parentContext = evt.Record.Headers.ExtractPropagationContext();
//             
//                 // https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
//                 var activity = ActivitySource.StartActivity(
//                     $"{evt.Record.Topic} receive",
//                     ActivityKind.Consumer,
//                     parentContext.ActivityContext
//                 );
//
//                 if (activity is null) return;
//
//                 activity
//                     .SetTag("messaging.system", "kafka")
//                     .SetTag("messaging.operation", "receive")
//                     .SetTag("messaging.destination_kind", "topic")
//                     .SetTag("messaging.destination", evt.Record.Topic)
//                     .SetTag("messaging.consumer_id", evt.Processor)
//                     .SetTag("messaging.message_id", evt.Record.Id.ToString())
//                     .SetTag("messaging.kafka.message_key", evt.Record.Key)
//                     .SetTag("messaging.kafka.client_id", evt.Processor)
//                     .SetTag("messaging.kafka.consumer_group", evt.Processor)
//                     .SetTag("messaging.kafka.partition", evt.Record.Partition.Value);
//
//                 if (InputActivities.TryAdd(evt.Record.Id, activity)) {
//                     activity.InjectHeaders(evt.Record.Headers);
//                 }
//                 // else {
//                 //     activity.Dispose();
//                 //     
//                 //     Logger.Warning(
//                 //         "{ProcessorName} | {RecordId} ## duplicate activity detected",
//                 //         evt.Processor, evt.Record.Id
//                 //     );
//                 //     
//                 // }
//             }
//         );
//         
//         // On<InputConsumed>(
//         //     evt => {
//         //         var parentContext = evt.Record.Headers.ExtractPropagationContext();
//         //     
//         //         // https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
//         //         var activity = ActivitySource.CreateActivity(
//         //             $"{evt.Record.Topic} consume",
//         //             ActivityKind.Consumer,
//         //             parentContext.ActivityContext
//         //         );
//         //
//         //         if (activity is null) return;
//         //     
//         //         var parentActivity = InputActivities[evt.Record.Id];
//         //
//         //         activity
//         //             .SetStartTime(evt.Record.)
//         //             .SetTag("messaging.system", "kafka")
//         //             .SetTag("messaging.operation", "receive")
//         //             .SetTag("messaging.destination_kind", "topic")
//         //             .SetTag("messaging.destination", evt.Record.Topic)
//         //             .SetTag("messaging.consumer_id", evt.Processor)
//         //             .SetTag("messaging.message_id", evt.Record.Id.ToString())
//         //             .SetTag("messaging.kafka.message_key", evt.Record.Key)
//         //             .SetTag("messaging.kafka.client_id", evt.Processor)
//         //             .SetTag("messaging.kafka.consumer_group", evt.Processor)
//         //             .SetTag("messaging.kafka.partition", evt.Record.Partition.Value);
//         //
//         //         if (InputActivities.TryAdd(evt.Record.Id, activity)) {
//         //             activity.InjectHeaders(evt.Record.Headers);
//         //         }
//         //         
//         //         //
//         //         // if (InputActivities.TryRemove(evt.Record.Id, out var activity)) {
//         //         //     activity
//         //         //         .SetTag("messaging.acknowledge_type", "Success")
//         //         //         .Dispose();
//         //         // }
//         //         // else {
//         //         //     Logger.Error(
//         //         //         "{ProcessorName} | {RecordId} ## failed to find start of activity",
//         //         //         evt.Processor, evt.Record.Id
//         //         //     );
//         //         // }
//         //         //
//         //         // foreach (var outputMessage in evt.Output)
//         //         //     inFlightOutput.TryAdd(outputMessage.RequestId, evt.Record.Id);
//         //         //
//         //         // Logger
//         //         //     //.WithRecordInfo(evt.Record)
//         //         //     .Verbose(
//         //         //         "{Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
//         //         //         nameof(InputConsumed), evt.Record, evt.Record.MessageType.Name,
//         //         //         evt.Output.Count, stopwatch!.Elapsed
//         //         //     );
//         //     }
//         // );
//
//         On<InputError>(
//             evt => {
//                 if (InputActivities.TryRemove(evt.Record.Id, out var activity)) {
//                     activity
//                         .SetTag("messaging.acknowledge_type", "Error")
//                         .RecordException(evt.Exception);
//                 }
//                 else {
//                     Logger.Error(
//                         "{Event} {RecordId} {MessageType} ## failed to find start of activity",
//                         nameof(InputError), evt.Record, evt.Record.MessageType.Name
//                     );
//                 }
//             }
//         );
//
//         // On<OutputProcessed>(
//         //     evt => {
//         //         if (evt.Result.Success) {
//         //             Logger
//         //                 // .WithRecordInfo(evt.Input)    
//         //                 .Verbose(
//         //                     "{Event} {RecordId} | {RequestId} {MessageType} >> {OutputRecordId}",
//         //                     nameof(OutputProcessed), evt.Input, evt.Request.RequestId,
//         //                     evt.Request.Message.GetType().Name,
//         //                     $"{evt.Result.RecordId.Topic}:{evt.Result.RecordId.Partition}@{evt.Result.RecordId.Offset}"
//         //                 );
//         //         }
//         //         else {
//         //             Logger
//         //                 // .WithRecordInfo(evt.Input)
//         //                 .Error(
//         //                     evt.Result.Exception,
//         //                     "{Event} {RecordId} | {RequestId} ## {ErrorMessage}",
//         //                     nameof(OutputProcessed), evt.Input, evt.Request.RequestId,
//         //                     evt.Result.Exception!.Message
//         //                 );
//         //         }
//         //     }
//         // );
//         
//         // On<BeforeProduce>(
//         //     evt => {
//         //         var activity = ActivitySource.StartActivity($"{evt.Request.Topic} send", ActivityKind.Producer);
//         //         
//         //         // check for interested listeners and return if none
//         //         if (activity is null) return;
//         //
//         //         activity
//         //             .SetTag("messaging.system", "kafka")
//         //             .SetTag("messaging.operation", "send")
//         //             .SetTag("messaging.destination_kind", "topic")
//         //             .SetTag("messaging.destination", evt.Request.Topic)
//         //             .SetTag("messaging.producer_id", evt.ProducerName)
//         //             .SetTag("messaging.kafka.message_key", evt.Request.Key)
//         //             .SetTag("messaging.kafka.client_id", evt.ProducerName);
//         //         
//         //         if (OutputActivities.TryAdd(evt.Request.RequestId, activity)) {
//         //             activity.InjectHeaders(evt.Request.Headers);
//         //         }
//         //         // just for debug since duplicate could mean a retry of sorts... test and investigate...
//         //         else {
//         //             activity
//         //                 .SetTag("messaging.acknowledge_type", "Duplicate")
//         //                 .Dispose();
//         //                 
//         //             Logger.Warning(
//         //                 "{ProducerName} | {RequestId} | duplicate activity detected",
//         //                 evt.ProducerName, evt.Request.RequestId
//         //             );
//         //         }
//         //     }
//         // );
//         //
//         // On<ProduceResultReceived>(
//         //     evt => {
//         //         if (OutputActivities.TryRemove(evt.Result.RequestId, out var activity)) {
//         //             if (evt.Result.Success) {
//         //                 activity
//         //                     .SetTag("messaging.acknowledge_type", "Success")
//         //                     .SetTag("messaging.message_id", evt.Result.RecordId.ToString())
//         //                     .SetTag("messaging.kafka.partition", evt.Result.RecordId.Partition);
//         //             }
//         //             else {
//         //                 activity
//         //                     .SetTag("messaging.acknowledge_type", "Error")
//         //                     .RecordException(evt.Result.Exception);
//         //             }
//         //             
//         //             activity.Dispose();
//         //         }
//         //         else {
//         //             Logger.Error(
//         //                 evt.Result.Exception,
//         //                 "{ProducerName} | {RequestId} | failed to find start of activity",
//         //                 evt.ProducerName, evt.Result.RequestId
//         //             );
//         //         }
//         //     }
//         // );
//
//         On<InputProcessed>(
//             evt => {
//                 if (InputActivities.TryRemove(evt.Record.Id, out var activity)) {
//                     activity.Dispose();
//                 }
//                 else {
//                     Logger.Error(
//                         "{ProcessorName} | {RecordId} ## failed to find start of activity",
//                         evt.Processor, evt.Record.Id
//                     );
//                 }
//             }
//         );
//         
//     }
//
//     ActivitySource                           ActivitySource   { get; }
//     ConcurrentDictionary<RecordId, Activity> InputActivities  { get; }
//     ConcurrentDictionary<Guid, Activity>     OutputActivities { get; }
// }