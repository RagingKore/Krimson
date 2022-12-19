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
                // [thrd:app]: Configuration property `sasl.mechanism` set to `PLAIN` but `security.protocol` is not configured for SASL: recommend setting `security.protocol` to SASL_SSL or SASL_PLAINTEXT
                //evt.LogMessage.Message.StartsWith("[thrd:app]: Configuration property `sasl.mechanism` set to `PLAIN`")

                Logger.Write(
                    evt.LogMessage.GetLogLevel(),
                    $"{{ClientInstanceId}} | {{Source}} {evt.LogMessage.Message}",
                    evt.ClientInstanceId, evt.LogMessage.Facility
                );
            }
        );

        On<ConfluentProducerError>(
            evt => {
                var logLevel = evt.Error.IsTerminal()
                    ? LogEventLevel.Fatal
                    : evt.Error.IsTransient()
                        ? LogEventLevel.Information
                        : LogEventLevel.Debug;

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