// ReSharper disable TemplateIsNotCompileTimeConstantProblem

using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Logging;
using Serilog.Events;

namespace Krimson.Producers.Interceptors;

public sealed class ConfluentProducerLogger : InterceptorModule {
    public ConfluentProducerLogger() {
        On<ConfluentProducerLog>(
            evt => {
                Logger.Write(
                    evt.LogMessage.GetLogLevel(), 
                    $"{{ClientInstanceId}} | {{Source}} {evt.LogMessage.Message}", 
                    evt.ClientInstanceId, evt.LogMessage.Facility
                );
            }
        );

        On<ConfluentProducerError>(
            evt => {
                // TODO SS: sanity check might not be needed... investigate
                var logLevel = evt.Error.IsUseless()
                    ? LogEventLevel.Debug
                    : evt.Error.IsTerminal()
                        ? LogEventLevel.Fatal
                        : LogEventLevel.Error;

                var source = evt.Error.IsLocalError 
                    ? ConfluentKafkaErrorSource.Client 
                    : ConfluentKafkaErrorSource.Broker;

                Logger.Write(
                    level: logLevel, exception: new KafkaException(evt.Error), 
                    messageTemplate: "{ClientInstanceId} | {Source} ({Error}) {ErrorCode} {ErrorReason}",
                    evt.ClientInstanceId, source, (int) evt.Error.Code, evt.Error.Code, evt.Error.Reason
                );
            }
        );
    }
}