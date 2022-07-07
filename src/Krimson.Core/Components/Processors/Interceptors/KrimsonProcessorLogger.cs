using System.Collections.Concurrent;
using System.Diagnostics;
using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Producers.Interceptors;

namespace Krimson.Processors.Interceptors;

[PublicAPI]
class KrimsonProcessorLogger : InterceptorModule {
    public KrimsonProcessorLogger() {
        On<ProcessorActivated>(
            evt => {
                Logger.Information(
                    "{Event} {GroupId} :: {Topics}", 
                    nameof(ProcessorActivated), evt.Processor.GroupId, evt.Processor.Topics
                );
            }
        );

        On<ProcessorTerminating>(
            evt => {
                Logger.Verbose(
                    "{Event} {GroupId} :: {Topics}", 
                    nameof(ProcessorTerminating), evt.Processor.GroupId, evt.Processor.Topics
                );
            }
        );

        On<ProcessorTerminated>(
            evt => {
                if (evt.Exception is null)
                    Logger.Debug(
                        "{Event} {GroupId} :: {Topics}",
                        nameof(ProcessorTerminated), evt.Processor.GroupId, evt.Processor.Topics
                    );
                else
                    Logger.Debug(
                        evt.Exception, "{Event} {GroupId} :: {Topics} ## {ErrorMessage}",
                        "ProcessorViolentlyTerminated", evt.Processor.GroupId, evt.Processor.Topics, evt.Exception.Message
                    );
            }
        );

        On<ProcessorTerminatedUserHandlingError>(
            evt => {
                Logger.Warning(evt.Exception, "user handling error: {ErrorMessage}", evt.Exception!.Message);
            }
        );

        On<PartitionsAssigned>(
            evt => {
                foreach (var group in evt.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
                    Logger.Information(
                        "{Event} {Topic} << ({PartitionCount}) {Partitions}",
                        nameof(PartitionsAssigned), group.Key, group.Count(), group.Select(x => x.Partition.Value)
                    );
            }
        );

        On<PartitionsRevoked>(
            evt => {
                var positionsByTopic = evt.Positions
                    .OrderBy(x => x.Topic)
                    .ThenBy(x => x.Partition.Value)
                    .GroupBy(x => x.Topic);
                
                foreach (var positions in positionsByTopic) {
                    var unset = positions.Where(x => x.Offset == Offset.Unset).ToList();
                    var acked = positions.Except(unset).ToList();

                    if (unset.Any()) {
                        Logger.Warning(
                            "{Event} {Topic} !! ({PartitionCount}) {AckedPositions} {UnsetPositions}",
                            nameof(PartitionsRevoked), positions.Key, unset.Count,
                            acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
                            unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                        );
                    }
                    else {
                        Logger.Warning(
                            "{Event} {Topic} !! ({PartitionCount}) {AckedPositions}",
                            nameof(PartitionsRevoked), positions.Key, unset.Count,
                            acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                        );
                    }
                }
            }
        );

        On<PartitionsLost>(
            evt => {
                foreach (var group in evt.Positions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
                    Logger.Warning(
                        "{Event} {Topic} ?? ({PartitionCount}) {Partitions}",
                        nameof(PartitionsLost), group.Key, group.Count(),
                        group.Select(x => x.Partition.Value)
                    );
            }
        );

        On<PartitionEndReached>(
            evt => {
                Logger.Information(
                    "{Event} {Topic} |> [{Partition}] @ {Offset}",
                    nameof(PartitionEndReached), evt.Position.Topic,
                    evt.Position.Partition.Value, evt.Position.Offset.Value
                );
            }
        );
        
        On<PositionsCommitted>(
            evt => {
                if (evt.Positions.Any()) {
                    var positionsByTopic = evt.Positions
                        .OrderBy(x => x.Topic)
                        .ThenBy(x => x.Partition.Value)
                        .GroupBy(x => x.Topic);

                    foreach (var positions in positionsByTopic) {
                        var unset  = positions.Where(x => x.Offset == Offset.Unset).ToList();
                        var acked  = positions.Except(unset).ToList();
                        //var failed = positions.Where(x => x.Error.IsTerminal()).Except(unset).ToList();

                        if (unset.Any()) {
                            Logger.Information(
                                "{Event} {Topic} >> ({PositionsCount}) {AckedPositions} {UnsetPositions}",
                                nameof(PositionsCommitted), positions.Key, unset.Count,
                                acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
                                unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                            );
                        }
                        else {
                            Logger.Information(
                                "{Event} {Topic} >> ({PositionsCount}) {AckedPositions}",
                                nameof(PositionsCommitted), positions.Key, unset.Count,
                                acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                            );
                        }
                    }
                }
                
                if (evt.Error.IsError) {
                    var kex = evt.Error.AsKafkaException()!;
                    Logger.Error(kex, "{Event} ## {ErrorMessage}", "PositionsCommitError", kex.Message);
                }
            }
        );

        On<InputSkipped>(
            evt => {
                Logger
                    .Verbose(
                        "{RecordId} message {MessageType} skipped",
                        evt.Record, evt.Record.MessageType.Name
                    );
            }
        );

        var inProcessRecords = new ConcurrentDictionary<RecordId, Stopwatch>();
        
        On<InputReady>(
            evt => {
                inProcessRecords.TryAdd(evt.Record.Id, Stopwatch.StartNew());

                 Logger
                     .Verbose(
                        "{Event} {RecordId} {MessageType}",
                        nameof(InputReady), evt.Record, evt.Record.MessageType.Name
                    );
            }
        );

        var inFlightOutput = new ConcurrentDictionary<Guid, RecordId>();
        
        On<InputConsumed>(
            evt => {
                inProcessRecords.TryGetValue(evt.Record.Id, out var stopwatch);

                foreach (var outputMessage in evt.Output)
                    inFlightOutput.TryAdd(outputMessage.RequestId, evt.Record.Id);

                Logger
                    .Verbose(
                        "{Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
                        nameof(InputConsumed), evt.Record, evt.Record.MessageType.Name,
                        evt.Output.Count, stopwatch!.Elapsed
                    );
            }
        );
        
        On<InputError>(
            evt => {
                Logger
                    .Verbose(
                        "{Event} {RecordId} {MessageType} ## {ErrorMessage}",
                        nameof(InputError), evt.Record, evt.Record.MessageType.Name,
                        evt.Exception.Message
                    );
            }
        );

        On<OutputProcessed>(
            evt => {
                if (evt.Result.Success) {
                    Logger
                        .Verbose(
                            "{Event} {RecordId} | {RequestId} {MessageType} >> {OutputRecordId}",
                            nameof(OutputProcessed), evt.Input, evt.Request.RequestId, evt.Request.Message.GetType().Name,
                            $"{evt.Result.RecordId.Topic}:{evt.Result.RecordId.Partition}@{evt.Result.RecordId.Offset}"
                        );
                }
                else {
                    Logger
                        .Error(
                            evt.Result.Exception, 
                            "{Event} {RecordId} | {RequestId} ## {ErrorMessage}",
                            nameof(OutputProcessed), evt.Input, evt.Request.RequestId, evt.Result.Exception!.Message
                        );
                }
            }
        );
        
        On<InputProcessed>(
            evt => {
                inProcessRecords.TryRemove(evt.Record.Id, out var stopwatch);

                Logger
                    .Debug(
                        "{Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
                        nameof(InputProcessed), evt.Record, evt.Record.MessageType.Name,
                        evt.Output.Count, stopwatch!.Elapsed
                    );
            }
        );
        
        On<ProduceResultError>(
            evt => {
                Logger.Error(
                    evt.Exception,
                    "{ProducerName} | {RequestId} | failed handling callback: {ErrorMessage}",
                    evt.ProducerName, evt.RequestId, evt.Exception.Message
                );
            }
        );
    }
}