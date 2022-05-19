using Confluent.Kafka;
using Krimson.Interceptors;
using Serilog.Events;

namespace Krimson.Producers.Interceptors;

public sealed class LoggingProducerInterceptor : InterceptorModule {
    public LoggingProducerInterceptor(ILogger log) {

        On<OnConfluentProducerError>(
            evt => {
                var level  = evt.Exception.Error.IsFatal ? LogEventLevel.Fatal : LogEventLevel.Warning;
                var source = evt.Exception.Error.IsLocalError ? "Client" : "Broker";
        
                log.ForContext(nameof(Error), evt.Exception.Error).Write(
                    level, evt.Exception, "[{ProcessorName}] {Source} {ErrorCode} {ErrorReason}",
                    evt.ProducerName, source, evt.Exception.Error.Code, evt.Exception.Error
                );
            }
        );

        On<OnProduce>(
            evt => {
                log.Verbose(
                    "[{ProducerName}] sending {RequestId} {MessageType} ({Key}) >> {Topic}",
                    evt.ProducerName, evt.Request.RequestId, 
                    evt.Request.Message.GetType().Name, 
                    evt.Request.Key, evt.Request.Topic
                );
            }
        );

        On<OnProduceResult>(
            evt => {
                if (evt.Result.DeliveryFailed) {
                    log.Error(
                        evt.Result.Exception,
                        "[{ProducerName}] failed to send {RequestId} :: {ErrorMessage}",
                        evt.ProducerName, evt.Result.RequestId, evt.Result.Exception!.Message
                    );
                }
                else {
                    // log.Verbose(
                    //     "[{ProducerName}] delivered {MessageType} ({Key}) >> {Topic} [{Partition}] @ {Offset} : {RequestId}",
                    //     evt.ProducerName, ack.Request.Message.GetType().Name, ack.Request.Key,
                    //     ack.Position.Topic, ack.Position.Partition.Value, ack.Position.Offset.Value,
                    //     ack.Request.RequestId
                    // );
                }
            }
        );

        On<OnProduceResultUserException>(
            evt => {
                // log.Verbose(
                //     "[{ProducerName}] sending {MessageType} ({Key}) >> {Topic} : {RequestId}",
                //     evt.ProducerName, evt.Message.Message.GetType().Name, evt.Message.Key,
                //     evt.Message.Topic,
                //     evt.Message.RequestId
                // );

                // log.Warning(
                //     "[{ProducerName}] failed handling producer result callback ({Key}) >> {Topic} : {RequestId} :: {ErrorMessage}",
                //     evt.ProducerName, evt.Message.GetType().Name, evt.Message.Key,
                //     result.Message.Key, result.Message.Topic, exception.Message
                // );
            }
        );
    }
}