using System.Collections.Concurrent;
using System.Diagnostics;
using Krimson.Interceptors;
using Krimson.OpenTelemetry.Trace;
using Krimson.Processors.Interceptors;
using OpenTelemetry.Trace;

namespace Krimson.OpenTelemetry;

[PublicAPI]
public class OpenTelemetryProcessorInterceptor : InterceptorModule {
    public OpenTelemetryProcessorInterceptor(string sourceName) {
        Activities     = new ConcurrentDictionary<RecordId, Activity>();
        ActivitySource = new ActivitySource(sourceName);

        On<InputReady>(
            evt => {
                var parentContext = evt.Record.Headers.ExtractPropagationContext();
            
                // https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L68
                var activity = ActivitySource.StartActivity(
                    $"{evt.Record.Topic} process",
                    ActivityKind.Consumer,
                    parentContext.ActivityContext
                );

                if (activity is null) return;

                activity
                    .SetTag("messaging.system", "kafka")
                    .SetTag("messaging.operation", "process")
                    .SetTag("messaging.destination_kind", "topic")
                    .SetTag("messaging.destination", evt.Record.Topic)
                    .SetTag("messaging.consumer_id", evt.Processor.ClientId)
                    .SetTag("messaging.message_id", evt.Record.Id.ToString())
                    .SetTag("messaging.kafka.message_key", evt.Record.Key)
                    .SetTag("messaging.kafka.client_id", evt.Processor.ClientId)
                    .SetTag("messaging.kafka.consumer_group", evt.Processor.GroupId)
                    .SetTag("messaging.kafka.partition", evt.Record.Partition.Value);

                if (!Activities.TryAdd(evt.Record.Id,  activity.InjectHeaders(evt.Record.Headers))) {
                    activity.Dispose();
                    
                    Logger.Warning(
                        "{ProcessorName} | {RecordId} ## duplicate activity detected",
                        evt.Processor.ClientId, evt.Record.Id
                    );
                }
            }
        );
        
        On<InputError>(
            evt => {
                if (Activities.TryRemove(evt.Record.Id, out var activity)) {
                    activity
                        .SetTag("messaging.acknowledge_type", "Error")
                        .RecordException(evt.Exception);
                }
                else {
                    Logger.Error(
                        "{Event} {RecordId} {MessageType} ## failed to find start of activity",
                        nameof(InputError), evt.Record, evt.Record.MessageType.Name
                    );
                }
            }
        );

        On<InputProcessed>(
            evt => {
                if (Activities.TryRemove(evt.Record.Id, out var activity)) {
                    activity.Dispose();
                }
                else {
                    Logger.Error(
                        "{ProcessorName} | {RecordId} ## failed to find start of activity",
                        evt.Processor.ClientId, evt.Record.Id
                    );
                }
            }
        );
        
    }

    ActivitySource                           ActivitySource { get; }
    ConcurrentDictionary<RecordId, Activity> Activities     { get; }
}