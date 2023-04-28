using System.Collections.Concurrent;
using System.Diagnostics;
using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Producers.Interceptors;

namespace Krimson.Processors.Interceptors;

[PublicAPI]
class KrimsonProcessorLogger : InterceptorModule {
    public KrimsonProcessorLogger() {
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
        
        On<ProcessorActivated>(
            evt => {
                Logger.Information(
                    "{ProcessorName} {Event}", 
                    evt.Processor.ClientId, nameof(ProcessorActivated)
                );
            }
        );

        On<ProcessorTerminating>(
            evt => {
                Logger.Verbose(
                    "{ProcessorName} {Event}", 
                    evt.Processor.ClientId, nameof(ProcessorTerminating)
                );
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
                    Logger.Error(
                        evt.Exception, "{ProcessorName} {Event} {ErrorMessage}",
                        evt.Processor.ClientId, "ProcessorViolentlyTerminated", evt.Exception.Message
                    );
            }
        );

        On<ProcessorTerminatedUserHandlingError>(
            evt => {
                Logger.Warning(
                    evt.Exception, "{ProcessorName} {Event} user handling error: {ErrorMessage}", 
                    evt.Processor.ClientId, "ProcessorTerminatedUserHandlingError", evt.Exception!.Message
                );
            }
        );

        On<PartitionsAssigned>(
            evt => {
                foreach (var group in evt.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
                    Logger.Information(
                        "{ProcessorName} {Event} {Topic} << ({PartitionCount}) {Partitions}",
                        evt.Processor.ClientId, nameof(PartitionsAssigned), group.Key, group.Count(), group.Select(x => x.Partition.Value)
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
                            "{ProcessorName} {Event} {Topic} !! ({PartitionCount}) {AckedPositions} {UnsetPositions}",
                            evt.Processor.ClientId, nameof(PartitionsRevoked), positions.Key, unset.Count,
                            acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
                            unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                        );
                    }
                    else {
                        Logger.Warning(
                            "{ProcessorName} {Event} {Topic} !! ({PartitionCount}) {AckedPositions}",
                            evt.Processor.ClientId, nameof(PartitionsRevoked), positions.Key, unset.Count,
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
                        "{ProcessorName} {Event} {Topic} ?? ({PartitionCount}) {Partitions}",
                        evt.Processor.ClientId, nameof(PartitionsLost), group.Key, group.Count(),
                        group.Select(x => x.Partition.Value)
                    );
            }
        );

        On<PartitionEndReached>(
            evt => {
                Logger.Information(
                    "{ProcessorName} {Event} {Topic} |> [{Partition}] @ {Offset}",
                    evt.Processor.ClientId, nameof(PartitionEndReached), evt.Position.Topic,
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
                                "{ProcessorName} {Event} {Topic} >> ({PositionsCount}) {AckedPositions} {UnsetPositions}",
                                evt.Processor.ClientId, nameof(PositionsCommitted), positions.Key, unset.Count,
                                acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
                                unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                            );
                        }
                        else {
                            Logger.Information(
                                "{ProcessorName} {Event} {Topic} >> ({PositionsCount}) {AckedPositions}",
                                evt.Processor.ClientId, nameof(PositionsCommitted), positions.Key, unset.Count,
                                acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                            );
                        }
                    }
                }
                
                if (evt.Error.IsError) {
                    var kex = evt.Error.AsKafkaException()!;
                    Logger.Error(kex, "{ProcessorName} {Event} {ErrorMessage}", evt.Processor.ClientId, "PositionsCommitError", kex.Message);
                }
            }
        );

        On<InputSkipped>(
            evt => {
                Logger.Verbose(
                    "{ProcessorName} {RecordId} message {MessageType} skipped",
                    evt.Processor.ClientId, evt.Record, evt.Record.MessageType.Name
                );
            }
        );

        var inProcessRecords = new ConcurrentDictionary<RecordId, Stopwatch>();
        
        On<InputReady>(
            evt => {
                inProcessRecords.TryAdd(evt.Record.Id, Stopwatch.StartNew());

                Logger.Verbose(
                    "{ProcessorName} {Event} {RecordId} {MessageType}",
                    evt.Processor.ClientId, nameof(InputReady), evt.Record, evt.Record.MessageType.Name
                );
            }
        );

        var inFlightOutput = new ConcurrentDictionary<Guid, RecordId>();
        
        On<InputConsumed>(
            evt => {
                inProcessRecords.TryGetValue(evt.Record.Id, out var stopwatch);

                foreach (var outputMessage in evt.Output)
                    inFlightOutput.TryAdd(outputMessage.RequestId, evt.Record.Id);

                Logger.Verbose(
                    "{ProcessorName} {Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
                    evt.Processor.ClientId, nameof(InputConsumed), evt.Record, evt.Record.MessageType.Name,
                    evt.Output.Count, stopwatch!.Elapsed
                );
            }
        );
        
        On<InputError>(
            evt => {
                Logger.Verbose(
                    "{ProcessorName} {Event} {RecordId} {MessageType} {ErrorMessage}",
                    evt.Processor.ClientId, nameof(InputError), evt.Record, evt.Record.MessageType.Name,
                    evt.Exception.Message
                );
            }
        );

        On<OutputProcessed>(
            evt => {
                if (evt.Result.Success) {
                    Logger.Verbose(
                        "{ProcessorName} {Event} {RecordId} {RequestId} {MessageType} >> {OutputRecordId}",
                        evt.Processor.ClientId, nameof(OutputProcessed), evt.Input, evt.Request.RequestId, evt.Request.Message.GetType().Name,
                        $"{evt.Result.RecordId.Topic}:{evt.Result.RecordId.Partition}@{evt.Result.RecordId.Offset}"
                    );
                }
                else {
                    Logger.Error(
                        evt.Result.Exception, 
                        "{ProcessorName} {Event} {RecordId} {RequestId} {ErrorMessage}",
                        evt.Processor.ClientId, nameof(OutputProcessed), evt.Input, evt.Request.RequestId, evt.Result.Exception!.Message
                    );
                }
            }
        );
        
        On<InputProcessed>(
            evt => {
                inProcessRecords.TryRemove(evt.Record.Id, out var stopwatch);

                Logger.Debug(
                    "{ProcessorName} {Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
                    evt.Processor.ClientId, nameof(InputProcessed), evt.Record, evt.Record.MessageType.Name,
                    evt.Output.Count, stopwatch!.Elapsed
                );
            }
        );
        
        On<ProduceResultError>(
            evt => {
                Logger.Error(
                    evt.Exception,
                    "{ProducerName} {RequestId} Failed handling callback: {ErrorMessage}",
                    evt.ProducerName, evt.RequestId, evt.Exception.Message
                );
            }
        );
    }
}