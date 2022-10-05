// ReSharper disable TemplateIsNotCompileTimeConstantProblem

using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Logging;
using Serilog.Events;

namespace Krimson.Client.Interceptors;

public sealed class ConfluentAdminClientLogger : InterceptorModule {
    public ConfluentAdminClientLogger() {
        On<ConfluentAdminClientLog>(
            evt => {
                Logger.Write(
                    evt.LogMessage.GetLogLevel(),
                    $"{{ClientInstanceId}} | {{Source}} {evt.LogMessage.Message}",
                    evt.ClientInstanceId, evt.LogMessage.Facility
                );
            }
        );

        On<ConfluentAdminClientError>(
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