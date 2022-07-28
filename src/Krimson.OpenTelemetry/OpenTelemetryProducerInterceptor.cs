// ReSharper disable TemplateIsNotCompileTimeConstantProblem

using System.Collections.Concurrent;
using System.Diagnostics;
using Krimson.Interceptors;
using Krimson.OpenTelemetry.Trace;
using Krimson.Producers.Interceptors;

namespace Krimson.OpenTelemetry;

public sealed class OpenTelemetryProducerInterceptor : InterceptorModule {
    public OpenTelemetryProducerInterceptor(string sourceName) {
        ActivitySource = new ActivitySource(sourceName);
        Activities     = new ConcurrentDictionary<Guid, Activity>();

        var hasListeners = true; // assume yes to run at least once
        
        On<BeforeProduce>(
            evt => {
                if (!hasListeners) return; // exit
                
                // might have parent trace
                var parentContext = evt.Request.Headers.ExtractPropagationContext();

                var activity = ActivitySource.StartActivity(
                    $"{evt.Request.Topic} send",
                    ActivityKind.Producer,
                    parentContext.ActivityContext
                );
                
                // check for interested listeners and return if none
                if (activity is null) {
                    hasListeners = false;
                    
                    Logger.Debug(
                        "{ComponentName} {Event} {RequestId} activity has no event listeners",
                        evt.ProducerName, nameof(BeforeProduce), evt.Request.RequestId
                    );
                    
                    return;
                }

                activity
                    .SetTag("messaging.system", "kafka")
                    .SetTag("messaging.operation", "send")
                    .SetTag("messaging.destination_kind", "topic")
                    .SetTag("messaging.destination", evt.Request.Topic)
                    .SetTag("messaging.producer_id", evt.ProducerName)
                    .SetTag("messaging.kafka.client_id", evt.ProducerName)
                    .SetOptionalTag("messaging.kafka.message_key", evt.Request.Key);

                Activities.TryAdd(
                    evt.Request.RequestId, 
                    activity.InjectHeaders(evt.Request.Headers)
                ); 
                
                Logger.Debug(
                    "{ComponentName} {Event} {RequestId} activity has no event listeners",
                    evt.ProducerName, nameof(BeforeProduce), evt.Request.RequestId
                );
                
                // // just for debug since duplicate could mean a retry of sorts... test and investigate...
                // else {
                //     activity
                //         .SetTag("messaging.acknowledge_type", "Duplicate")
                //         .Dispose();
                //         
                //     Logger.Warning(
                //         "{ProducerName} | {RequestId} | duplicate activity detected",
                //         evt.ProducerName, evt.Request.RequestId
                //     );
                // }
            }
        );

        On<ProduceResultReceived>(
            evt => {
                if (!Activities.TryRemove(evt.Result.RequestId, out var activity)) return;

                if (evt.Result.Success) {
                    activity
                        .SetTag("messaging.acknowledge_type", "Success")
                        .SetTag("messaging.message_id", evt.Result.RecordId.ToString())
                        .SetTag("messaging.kafka.partition", evt.Result.RecordId.Partition);
                }
                else {
                    activity
                        .SetTag("messaging.acknowledge_type", "Error")
                        .SetException(evt.Result.Exception);
                }
                    
                activity.Dispose();
            }
        );
    }

    ActivitySource                       ActivitySource { get; }
    ConcurrentDictionary<Guid, Activity> Activities     { get; }

    public override ValueTask DisposeAsync() {
        foreach (var key in Activities.Keys.ToArray()) {
            if(Activities.TryRemove(key, out var activity))
                activity
                    .SetTag("messaging.acknowledge_type", "InterceptorDisposed")
                    .Dispose();
        }

        ActivitySource.Dispose();
        
        return ValueTask.CompletedTask;
    }
}