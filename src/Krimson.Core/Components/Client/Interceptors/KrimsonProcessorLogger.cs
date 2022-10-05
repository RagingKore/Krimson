using System.Collections.Concurrent;
using System.Diagnostics;
using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Producers.Interceptors;

namespace Krimson.Processors.Interceptors;

[PublicAPI]
class KrimsonClientLogger : InterceptorModule {
    public KrimsonClientLogger() {
        On<ProcessorConfigured>(
            evt => {
                Logger.Information(
                    "{ProcessorName} Subscribing to {Topics} as member of {GroupId}", 
                    evt.Processor.ClientId, evt.Processor.Topics, evt.Processor.GroupId
                );
                
                foreach (var (moduleName, key) in evt.Processor.RoutingKeys) {
                    Logger.Information(
                        "{ProcessorName} Module {Module} consumes {RoutingKey}", 
                        evt.Processor.ClientId, moduleName, key
                    );
                }
            }
        );
        
        On<ProcessorTerminated>(
            evt => {
                if (evt.Exception is null)
                    Logger.Debug(
                        "{ProcessorName} {Event}",
                        evt.Processor.ClientId, nameof(ProcessorTerminated)
                    );
                else
                    Logger.Warning(
                        evt.Exception, "{ProcessorName} {Event} {ErrorMessage}",
                        evt.Processor.ClientId, "ProcessorViolentlyTerminated", evt.Exception.Message
                    );
            }
        );
    }
}