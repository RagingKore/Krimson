using Confluent.Kafka;
using Humanizer;
using Krimson.Interceptors;
using Serilog.Enrichers;
using Serilog.Events;

namespace Krimson.Processors.Interceptors;

[PublicAPI]
class LoggingProcessorInterceptor : InterceptorModule {
    public LoggingProcessorInterceptor(ILogger log) : this() => Log = log;

    public LoggingProcessorInterceptor() {
        On<ProcessorStarted>(
            evt => {
                Log.Information("started and subscribed to {Topics}", evt.Topics);
            }
        );

        On<ProcessorStopping>(
            evt => {
                Log.Debug("stopping...");
            }
        );

        On<ProcessorStopped>(
            evt => {
                if (evt.Error is null)
                    Log.Debug("stopped");
                else
                    Log.Warning(evt.Error, "stopped on error");
            }
        );

        On<ConfluentConsumerError>(
            evt => {
                var level  = evt.Exception.Error.IsFatal ? LogEventLevel.Fatal : LogEventLevel.Warning;
                var source = evt.Exception.Error.IsLocalError ? "Client" : "Broker";

                Log.Write(
                    level, evt.Exception, "{ConsumerName} consumer {Source} {ErrorCode} {ErrorReason}",
                    evt.ConsumerName, source, evt.Exception.Error.Code, evt.Exception.Error.Reason
                );
            }
        );

        On<ConfluentProducerError>(
            evt => {
                var level  = evt.Exception.Error.IsFatal ? LogEventLevel.Fatal : LogEventLevel.Warning;
                var source = evt.Exception.Error.IsLocalError ? "Client" : "Broker";

                Log.Write(
                    level, evt.Exception, "{ProducerName} producer {Source} {ErrorCode} {ErrorReason}",
                    evt.ProducerName, source, evt.Exception.Error.Code, evt.Exception.Error.Reason
                );
            }
        );

        On<PartitionsAssigned>(
            evt => {
                foreach (var group in evt.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
                    Log.Information(
                        "{Topic} - {PartitionCount} partition(s) assigned: {Partitions}",
                        group.Key, group.Count(), group.Select(x => x.Partition.Value)
                    );
            }
        );

        On<PartitionsRevoked>(
            evt => {
                foreach (var positionsByTopic in evt.Positions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic)) {
                    var unset = positionsByTopic.Where(x => x.Offset == Offset.Unset).ToList();
                    var acked = positionsByTopic.Except(unset).ToList();

                    Log.Warning(
                        "{Topic} - {PartitionCount} partition(s) revoked: {AckedPositions} - {UnsetPositions}",
                        positionsByTopic.Key, evt.Positions.Count,
                        acked.Select(x => $"{x.Partition.Value}:{x.Offset.Value}"),
                        unset.Select(x => $"{x.Partition.Value}:{x.Offset.Value}")
                    );
                }
            }
        );

        On<PartitionsLost>(
            evt => {
                foreach (var group in evt.Positions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
                    Log.Warning(
                        "{Topic} - {PartitionCount} partition(s) lost: {Partitions}",
                        group.Key, group.Count(), group.Select(x => $"{x.Partition.Value}:{x.Offset.Value}")
                    );
            }
        );

        On<PartitionEndReached>(
            evt => {
                Log.Verbose(
                    "caught up to {Topic} [{Partition}] @ {Offset}",
                    evt.Position.Topic, evt.Position.Partition.Value, evt.Position.Offset.Value
                );
            }
        );


        On<PositionsCommitted>(
            evt => {
                if (evt.Error is not null)
                    Log.Error(evt.Error, "failed to commit tracked positions :: {ErrorMessage}", evt.Error.Message);
                else if (evt.Positions.Any())
                    foreach (var positionsByTopic in evt.Positions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic)) {
                        var unset = positionsByTopic.Where(x => x.Offset == Offset.Unset).ToList();
                        var acked = positionsByTopic.Except(unset).ToList();

                        Log.Debug(
                            "{Topic} - {PositionsCount} positions(s) committed: {AckedPositions} - {UnsetPositions}",
                            positionsByTopic.Key, evt.Positions.Count,
                            acked.Select(x => $"{x.Partition.Value}:{x.Offset.Value}"),
                            unset.Select(x => $"{x.Partition.Value}:{x.Offset.Value}")
                        );
                    }
                else
                    Log.Verbose("no tracked positions to commit");
            }
        );

        On<InputSkipped>(
            evt => {
                Log.WithRecordInfo(evt.Record).Verbose(
                    "{Topic} [{Partition}] @ {Position} | {MessageType} skipped",
                    evt.Record.Topic, evt.Record.Partition.Value, evt.Record.Offset.Value, evt.Record.Value.GetType().Name
                );
            }
        );

        On<InputReady>(
            evt => {
                Log.WithRecordInfo(evt.Record).Verbose(
                    "{Topic} [{Partition}] @ {Position} | {MessageType} ready",
                    evt.Record.Topic, evt.Record.Partition.Value, evt.Record.Offset.Value, evt.Record.Value.GetType().Name
                );
            }
        );
        
        On<InputProcessed>(
            evt => {
                Log.WithRecordInfo(evt.Record).Verbose(
                    "{Topic} [{Partition}] @ {Position} | {MessageType} processed ({OutputCount} output message(s) generated) in {ElapsedHumanReadable}",
                    evt.Record.Topic, evt.Record.Partition.Value, evt.Record.Offset.Value,
                    evt.Record.Value.GetType().Name, evt.Output.Count, MicroProfiler.GetElapsedHumanReadable(evt.Timestamp)
                );
            }
        );

        On<InputError>(
            evt => {
                Log.WithRecordInfo(evt.Record).Error(
                    evt.Error,
                    "{Topic} [{Partition}] @ {Position} | {MessageType} error :: {ErrorMessage}",
                    evt.Record.Topic, evt.Record.Partition.Value, evt.Record.Offset.Value,
                    evt.Record.Value.GetType().Name, evt.Error.Message
                );
            }
        );
        
        On<OutputProcessed>(
            evt => {
                if (evt.Result.RecordPersisted) {
                    Log.WithRecordInfo(evt.Input).Verbose(
                        "{Topic} [{Partition}] @ {Offset} | {MessageType} output {RequestId} sent",
                        evt.Input.Topic, evt.Input.Partition.Value, evt.Input.Offset.Value, evt.Input.Value.GetType().Name, evt.Result.RequestId
                    );
                }
                else {
                    Log.WithRecordInfo(evt.Input).Error(
                        "{Topic} [{Partition}] @ {Offset} | {MessageType} output {RequestId} not sent :: {ErrorMessage}",
                        evt.Input.Topic, evt.Input.Partition.Value, evt.Input.Offset.Value,
                        evt.Input.Value.GetType().Name, evt.Result.RequestId, evt.Result.Exception!.Message
                    );
                }
            }
        );
        
        // On<InputPositionTracked>(
        //     evt => {
        //         log.Verbose(
        //             "[{ProcessorName}] {InputId} position tracked {Topic} [{Partition}] @ {Position}",
        //             evt.Processor, evt.Record.RequestId, 
        //             evt.Record.Position.Topic, evt.Record.Position.Partition.Value, evt.Record.Position.Offset.Value
        //         );
        //     }
        // );

    }

    ILogger Log { get; } = Serilog.Log.ForContext<KrimsonProcessor>();
}