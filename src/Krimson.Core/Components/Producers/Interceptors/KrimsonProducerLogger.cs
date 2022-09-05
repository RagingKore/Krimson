// ReSharper disable TemplateIsNotCompileTimeConstantProblem

using Krimson.Interceptors;

namespace Krimson.Producers.Interceptors;

public sealed class KrimsonProducerLogger : InterceptorModule {
    public KrimsonProducerLogger() {
        On<BeforeProduce>(
            evt => {
                Logger.Verbose(
                    "{ProducerName} {RequestId} sending {MessageType} ({Key}) >> {Topic}",
                    evt.ProducerName, evt.Request.RequestId,
                    evt.Request.Message.GetType().Name, evt.Request.Key, evt.Request.Topic
                );
            }
        );

        On<ProduceResultReceived>(
            evt => {
                if (!evt.Result.Success) {
                    Logger.Error(
                        evt.Result.Exception,
                        "{ProducerName} {RequestId} failed to send message: {ErrorMessage}",
                        evt.ProducerName, evt.Result.RequestId, evt.Result.Exception!.Message
                    );
                }
                else {
                    Logger.Debug(
                        evt.Result.Exception,
                        "{ProducerName} {RequestId} >> {Topic} [{Partition}] @ {Offset}",
                        evt.ProducerName, evt.Result.RequestId, 
                        evt.Result.RecordId.Topic, evt.Result.RecordId.Partition, evt.Result.RecordId.Offset
                    );
                }
            }
        );

        On<ProduceResultUserHandlingError>(
            evt => {
                Logger.Error(
                    evt.UserException,
                    "{ProducerName} {RequestId} failed handling callback: {ErrorMessage}",
                    evt.ProducerName, evt.Result.RequestId, evt.UserException.Message
                );
            }
        );

        On<ProduceResultError>(
            evt => {
                Logger.Error(
                    evt.Exception,
                    "{ProducerName} {RequestId} failed handling callback: {ErrorMessage}",
                    evt.ProducerName, evt.RequestId, evt.Exception.Message
                );
            }
        );
    }
}