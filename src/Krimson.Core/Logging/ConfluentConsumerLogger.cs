// ReSharper disable TemplateIsNotCompileTimeConstantProblem

using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Logging;
using Serilog.Events;

namespace Krimson.Processors.Interceptors;

public sealed class ConfluentProcessorLogger : InterceptorModule {
    public ConfluentProcessorLogger() {
        On<ConfluentConsumerLog>(
            evt => {
                Logger.Write(
                    evt.LogMessage.GetLogLevel(),
                    $"{{ClientInstanceId}} | {{Source}} {evt.LogMessage.Message}",
                    evt.ClientInstanceId, evt.LogMessage.Facility
                );
            }
        );

        On<ConfluentConsumerError>(
            evt => {
                // sanity check
                var logLevel = evt.Error.IsUseless()
                    ? LogEventLevel.Debug
                    : evt.Error.IsTerminal()
                        ? LogEventLevel.Fatal
                        : LogEventLevel.Error;

                var source = evt.Error.IsLocalError
                    ? ConfluentKafkaErrorSource.Client
                    : ConfluentKafkaErrorSource.Broker;

                Logger.Write(
                    logLevel, new KafkaException(evt.Error), "{ClientInstanceId} | {Source} ({Error}) {ErrorCode} {ErrorReason}",
                    evt.ClientInstanceId, source, (int)evt.Error.Code,
                    evt.Error.Code, evt.Error.Reason
                );
            }
        );
    }
}