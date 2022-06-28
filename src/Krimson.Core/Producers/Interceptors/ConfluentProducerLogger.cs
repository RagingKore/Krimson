// ReSharper disable TemplateIsNotCompileTimeConstantProblem

using Confluent.Kafka;
using Krimson.Interceptors;
using Microsoft.Extensions.Logging;

namespace Krimson.Producers.Interceptors;

public sealed class ConfluentProducerLogger : InterceptorModule {
    public ConfluentProducerLogger() {
        On<ConfluentProducerLog>(
            evt => {
                Logger.Log(
                    evt.LogMessage.GetLogLevel(), 
                    $"{{ClientInstanceId}} | {{Source}} {evt.LogMessage.Message}", 
                    evt.ClientInstanceId, evt.LogMessage.Facility
                );
            }
        );

        On<ConfluentProducerError>(
            evt => {
                var logLevel = evt.Error.IsUseless()
                    ? LogLevel.Debug
                    : evt.Error.IsTerminal()
                        ? LogLevel.Critical
                        : LogLevel.Error;

                var source = evt.Error.IsLocalError 
                    ? ConfluentKafkaErrorSource.Client 
                    : ConfluentKafkaErrorSource.Broker;

                Logger.Log(
                    logLevel, new KafkaException(evt.Error), "{ClientInstanceId} | {Source} ({Error}) {ErrorCode} {ErrorReason}",
                    evt.ClientInstanceId, source, (int) evt.Error.Code, evt.Error.Code, evt.Error.Reason
                );
            }
        );
    }
}