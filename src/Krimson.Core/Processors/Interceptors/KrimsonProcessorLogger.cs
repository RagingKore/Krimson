using System.Collections.Concurrent;
using System.Diagnostics;
using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Logging;
using Krimson.Producers.Interceptors;
using Microsoft.Extensions.Logging;

namespace Krimson.Processors.Interceptors;

[PublicAPI]
class KrimsonProcessorLogger : InterceptorModule {
    public KrimsonProcessorLogger() {
        On<ProcessorStarted>(
            evt => {
                Logger.LogInformation(
                    "{Event} {GroupId} :: {Topics}", 
                    nameof(ProcessorStarted), evt.SubscriptionName, evt.Topics
                );
            }
        );

        On<ProcessorStopping>(
            evt => {
                Logger.LogTrace(
                    "{Event} {GroupId} :: {Topics}", 
                    nameof(ProcessorStopping), evt.SubscriptionName, evt.Topics
                );
            }
        );

        On<ProcessorStopped>(
            evt => {
                if (evt.Exception is null)
                    Logger.LogDebug(
                        "{Event} {GroupId} :: {Topics}",
                        nameof(ProcessorStopped), evt.SubscriptionName, evt.Topics
                    );
                else
                    Logger.LogDebug(
                        evt.Exception, "{Event} {GroupId} :: {Topics} ## {ErrorMessage}",
                        "ProcessorStoppedViolently", evt.SubscriptionName, evt.Topics, evt.Exception.Message
                    );
            }
        );

        On<ProcessorStoppedUserHandlingError>(
            evt => {
                Logger.LogWarning(evt.Exception, "user handling error: {ErrorMessage}", evt.Exception!.Message);
            }
        );

        On<PartitionsAssigned>(
            evt => {
                foreach (var group in evt.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
                    Logger.LogInformation(
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
                        Logger.LogWarning(
                            "{Event} {Topic} !! ({PartitionCount}) {AckedPositions} {UnsetPositions}",
                            nameof(PartitionsRevoked), positions.Key, unset.Count,
                            acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
                            unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                        );
                    }
                    else {
                        Logger.LogWarning(
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
                    Logger.LogWarning(
                        "{Event} {Topic} ?? ({PartitionCount}) {Partitions}",
                        nameof(PartitionsLost), group.Key, group.Count(),
                        group.Select(x => x.Partition.Value)
                    );
            }
        );

        On<PartitionEndReached>(
            evt => {
                Logger.LogInformation(
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
                            Logger.LogInformation(
                                "{Event} {Topic} >> ({PositionsCount}) {AckedPositions} {UnsetPositions}",
                                nameof(PositionsCommitted), positions.Key, unset.Count,
                                acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
                                unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                            );
                        }
                        else {
                            Logger.LogInformation(
                                "{Event} {Topic} >> ({PositionsCount}) {AckedPositions}",
                                nameof(PositionsCommitted), positions.Key, unset.Count,
                                acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
                            );
                        }
                    }
                }
                
                if (evt.Error.IsError) {
                    var kex = evt.Error.AsKafkaException()!;
                    Logger.LogError(kex, "{Event} ## {ErrorMessage}", "PositionsCommitError", kex.Message);
                }
            }
        );

        On<InputSkipped>(
            evt => {
                using (Logger.WithRecordInfo(evt.Record))
                    Logger.LogTrace(
                        "{RecordId} message {MessageType} skipped",
                        evt.Record, evt.Record.MessageType.Name
                    );
            }
        );

        var inProcessRecords = new ConcurrentDictionary<RecordId, Stopwatch>();
        
        On<InputReady>(
            evt => {
                inProcessRecords.TryAdd(evt.Record.Id, Stopwatch.StartNew());

                using (Logger.WithRecordInfo(evt.Record))
                    Logger.LogTrace(
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

                using (Logger.WithRecordInfo(evt.Record))
                    Logger.LogTrace(
                        "{Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
                        nameof(InputConsumed), evt.Record, evt.Record.MessageType.Name,
                        evt.Output.Count, stopwatch!.Elapsed
                    );
            }
        );
        
        On<InputError>(
            evt => {
                using (Logger.WithRecordInfo(evt.Record))
                    Logger.LogTrace(
                        "{Event} {RecordId} {MessageType} ## {ErrorMessage}",
                        nameof(InputError), evt.Record, evt.Record.MessageType.Name,
                        evt.Exception.Message
                    );
            }
        );

        On<OutputProcessed>(
            evt => {
                using (Logger.WithRecordInfo(evt.Input))
                    if (evt.Result.Success) {
                        Logger.LogTrace(
                            "{Event} {RecordId} | {RequestId} {MessageType} >> {OutputRecordId}",
                            nameof(OutputProcessed), evt.Input, evt.Request.RequestId, evt.Request.Message.GetType().Name,
                            $"{evt.Result.RecordId.Topic}:{evt.Result.RecordId.Partition}@{evt.Result.RecordId.Offset}"
                        );
                    }
                    else {
                        Logger.LogError(
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

                using (Logger.WithRecordInfo(evt.Record))
                    Logger.LogDebug(
                        "{Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
                        nameof(InputProcessed), evt.Record, evt.Record.MessageType.Name,
                        evt.Output.Count, stopwatch!.Elapsed
                    );
            }
        );
        
        // On<InputPositionTracked>(
        //     evt => {
        //         log.LogTrace(
        //             "[{ProcessorName}] {InputId} position tracked {Topic} [{Partition}] @ {Position}",
        //             evt.Processor, evt.Record.RequestId, 
        //             evt.Record.Position.Topic, evt.Record.Position.Partition.Value, evt.Record.Position.Offset.Value
        //         );
        //     }
        // );

        //
        // On<BeforeProduce>(
        //     evt => {
        //      
        //         Logger.LogDebug(
        //             "sending {RequestId} {MessageType} ({Key}) >> {Topic}",
        //             evt.Request.RequestId, evt.Request.Message.GetType().Name, evt.Request.Key, evt.Request.Topic
        //         );
        //     }
        // );
        //
        // On<ProduceResultReceived>(
        //     evt => {
        //         inFlightOutput.TryRemove(evt.Result.RequestId, out var recordId);
        //         
        //         
        //         if (evt.Result.DeliveryFailed) {
        //             Logger.LogError(
        //                 evt.Result.Exception,
        //                 "failed to send {RequestId} :: {ErrorMessage}",
        //                 evt.Result.RequestId, evt.Result.Exception!.Message
        //             );
        //         }
        //         else {
        //             // log.Verbose(
        //             //     "[{ProducerName}] delivered {MessageType} ({Key}) >> {Topic} [{Partition}] @ {Offset} : {RequestId}",
        //             //     evt.ProducerName, ack.Request.Message.GetType().Name, ack.Request.Key,
        //             //     ack.Position.Topic, ack.Position.Partition.Value, ack.Position.Offset.Value,
        //             //     ack.Request.RequestId
        //             // );
        //         }
        //     }
        // );
        //
        // On<ProducerResultUserHandlingError>(
        //     evt => {
        //         Logger.LogError(
        //             evt.UserException,
        //             "failed handling producer callback for {RequestId} :: {ErrorMessage}",
        //             evt.Result.RequestId, evt.UserException.Message
        //         );
        //     }
        // );
        
        On<ProducerResultError>(
            evt => {
                Logger.LogError(
                    evt.Exception,
                    "{ProducerName} | {RequestId} | failed handling callback: {ErrorMessage}",
                    evt.ProducerName, evt.RequestId, evt.Exception.Message
                );
            }
        );
    }
}

//
// [PublicAPI]
// class KrimsonProcessorLogger : InterceptorModule {
//     public KrimsonProcessorLogger() {
//         On<ProcessorStarted>(
//             evt => {
//                 Logger.LogInformation(
//                     "{Event} {GroupId} :: {Topics}", 
//                     nameof(ProcessorStarted), evt.SubscriptionName, evt.Topics
//                 );
//             }
//         );
//
//         On<ProcessorStopping>(
//             evt => {
//                 Logger.LogTrace(
//                     "{Event} {GroupId} :: {Topics}", 
//                     nameof(ProcessorStopping), evt.SubscriptionName, evt.Topics
//                 );
//             }
//         );
//
//         On<ProcessorStopped>(
//             evt => {
//                 if (evt.Exception is null)
//                     Logger.LogDebug(
//                         "{Event} {GroupId} :: {Topics}",
//                         nameof(ProcessorStopped), evt.SubscriptionName, evt.Topics
//                     );
//                     //
//                     // Logger.LogDebug("processor stopped gracefully");
//                 else
//                     Logger.LogDebug(
//                         "{Event} {GroupId} :: {Topics}",
//                         "ProcessorStoppedViolently", evt.SubscriptionName, evt.Topics
//                     );
//                     // Logger.LogWarning(evt.Exception, "processor stopped violently");
//             }
//         );
//
//         On<ProcessorStoppedUserHandlingError>(
//             evt => {
//                 Logger.LogWarning(evt.Exception, "user handling error: {ErrorMessage}", evt.Exception!.Message);
//             }
//         );
//
//         On<PartitionsAssigned>(
//             evt => {
//                 // foreach (var group in evt.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
//                 //     Logger.LogInformation(
//                 //         "topic {Topic} {PartitionCount} partition(s) assigned: {Partitions}",
//                 //         group.Key, group.Count(), group.Select(x => x.Partition.Value)
//                 //     );
//
//                 // foreach (var group in evt.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
//                 //     Logger.LogInformation(
//                 //         "{PartitionCount} partition(s) assigned for topic {Topic}: {Partitions}",
//                 //         group.Count(), group.Key, group.Select(x => x.Partition.Value)
//                 //     );
//
//                 foreach (var group in evt.Partitions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
//                     Logger.LogInformation(
//                         "{Event} {Topic} << ({PartitionCount}) {Partitions}",
//                         nameof(PartitionsAssigned), group.Key, group.Count(), group.Select(x => x.Partition.Value)
//                     );
//             }
//         );
//
//         On<PartitionsRevoked>(
//             evt => {
//                 var positionsByTopic = evt.Positions
//                     .OrderBy(x => x.Topic)
//                     .ThenBy(x => x.Partition.Value)
//                     .GroupBy(x => x.Topic);
//                 
//                 foreach (var positions in positionsByTopic) {
//                     var unset = positions.Where(x => x.Offset == Offset.Unset).ToList();
//                     var acked = positions.Except(unset).ToList();
//
//                     if (unset.Any()) {
//                         Logger.LogWarning(
//                             "{Event} {Topic} !! ({PartitionCount}) {AckedPositions} // {UnsetPositions}",
//                             nameof(PartitionsRevoked), positions.Key, unset.Count,
//                             acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
//                             unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                         );
//                         //
//                         // Logger.LogWarning(
//                         //     "{PositionsCount} partition(s) revoked from topic {Topic}: {AckedPositions} <> {UnsetPositions}",
//                         //     evt.Positions.Count, positions.Key,
//                         //     acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
//                         //     unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                         // );
//                     }
//                     else {
//                         Logger.LogWarning(
//                             "{Event} {Topic} !! ({PartitionCount}) {AckedPositions}",
//                             nameof(PartitionsRevoked), positions.Key, unset.Count,
//                             acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                         );
//                         // Logger.LogWarning(
//                         //     "{PositionsCount} partition(s) revoked from topic {Topic}: {AckedPositions}",
//                         //     positions.Key, evt.Positions.Count,
//                         //     acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                         // );
//                     }
//                 }
//             }
//         );
//
//         On<PartitionsLost>(
//             evt => {
//                 foreach (var group in evt.Positions.OrderBy(x => x.Topic).ThenBy(x => x.Partition.Value).GroupBy(x => x.Topic))
//                     Logger.LogWarning(
//                         "{Event} {Topic} ?? ({PartitionCount}) {Partitions}",
//                         nameof(PartitionsLost), group.Key, group.Count(),
//                         group.Select(x => x.Partition.Value)
//                     );
//                     //
//                     // Logger.LogWarning(
//                     //     "{PartitionCount} partition(s) lost from topic {Topic}: {Partitions}",
//                     //      group.Count(), group.Key, 
//                     //     group.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                     // );
//             }
//         );
//
//         On<PartitionEndReached>(
//             evt => {
//                 Logger.LogInformation(
//                     "{Event} {Topic} |> [{Partition}] @ {Offset}",
//                     nameof(PartitionEndReached), evt.Position.Topic,
//                     evt.Position.Partition.Value, evt.Position.Offset.Value
//                 );
//                 
//                 // Logger.LogInformation(
//                 //     "subscription caught up to topic {Topic} [{Partition}] @ {Offset}",
//                 //     evt.Position.Topic, evt.Position.Partition.Value, evt.Position.Offset.Value
//                 // );
//             }
//         );
//         
//         On<PositionsCommitted>(
//             evt => {
//                 if (evt.Positions.Any()) {
//                     var positionsByTopic = evt.Positions
//                         .OrderBy(x => x.Topic)
//                         .ThenBy(x => x.Partition.Value)
//                         .GroupBy(x => x.Topic);
//
//                     foreach (var positions in positionsByTopic) {
//                         var unset  = positions.Where(x => x.Offset == Offset.Unset).ToList();
//                         var acked  = positions.Except(unset).ToList();
//                         //var failed = positions.Where(x => x.Error.IsTerminal()).Except(unset).ToList();
//
//                         if (unset.Any()) {
//                             Logger.LogInformation(
//                                 "{Event} {Topic} >> ({PositionsCount}) {AckedPositions} // {UnsetPositions}",
//                                 nameof(PositionsCommitted), positions.Key, unset.Count,
//                                 acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
//                                 unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                             );
//                             
//                             // Logger.LogDebug(
//                             //     "{PositionsCount} positions(s) committed for topic {Topic}: {AckedPositions} <> {UnsetPositions}",
//                             //     evt.Positions.Count, positions.Key, 
//                             //     acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}"),
//                             //     unset.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                             // );
//                         }
//                         else {
//                             Logger.LogInformation(
//                                 "{Event} {Topic} >> ({PositionsCount}) {AckedPositions}",
//                                 nameof(PositionsCommitted), positions.Key, unset.Count,
//                                 acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                             );
//                             // Logger.LogDebug(
//                             //     "{PositionsCount} positions(s) committed for topic {Topic}: {AckedPositions}",
//                             //     evt.Positions.Count, positions.Key,
//                             //     acked.Select(x => $"{x.Partition.Value}@{x.Offset.Value}")
//                             // );
//                         }
//                     }
//                 }
//                 //
//                 // if (evt.Exception is not null) {
//                 //     if (evt.Exception.Error.Code != ErrorCode.NoError) {
//                 //         logger.LogError(evt.Exception, "Failed to commit tracked positions :: {ErrorMessage}", evt.Exception.Message);
//                 //     }
//                 // }
//                 else if (evt.Exception is not null) {
//                     Logger.LogError(evt.Exception, "{Event} ## {ErrorMessage}", "PositionsCommitError", evt.Exception.Message);
//                 }
//                 else {
//                     Logger.LogTrace("{Event} no tracked positions to commit", nameof(PositionsCommitted));
//                 }
//             }
//         );
//
//         On<InputSkipped>(
//             evt => {
//                 using (Logger.WithRecordInfo(evt.Record))
//                     Logger.LogTrace(
//                         "{RecordId} message {MessageType} skipped",
//                         evt.Record, evt.Record.MessageType.Name
//                     );
//             }
//         );
//
//         var inProcessMessages = new ConcurrentDictionary<RecordId, Stopwatch>();
//         
//         On<InputReady>(
//             evt => {
//                 inProcessMessages.TryAdd(evt.Record.Id, Stopwatch.StartNew());
//                 
//                 using (Logger.WithRecordInfo(evt.Record))
//                     Logger.LogTrace(
//                         "{RecordId} message {MessageType} ready",
//                         evt.Record, evt.Record.MessageType.Name
//                     );
//             }
//         );
//
//         On<InputConsumed>(
//             evt => {
//                 inProcessMessages.TryRemove(evt.Record.Id, out var stopwatch);
//
//                 using (Logger.WithRecordInfo(evt.Record)) {
//                     Logger.LogDebug(
//                         "{Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
//                         nameof(InputConsumed), evt.Record, evt.Record.MessageType.Name,
//                         evt.Output.Count, stopwatch!.Elapsed
//                     );
//                 }
//             }
//         );
//
//         // On<InputProcessed>(
//         //     evt => {
//         //         inProcessMessages.TryRemove(evt.Record.Id, out var stopwatch);
//         //
//         //         using (Logger.WithRecordInfo(evt.Record)) {
//         //             Logger.LogDebug(
//         //                 "{Event} {RecordId} {MessageType} ({OutputCount} output) {Elapsed}",
//         //                 nameof(InputProcessed), evt.Record, evt.Record.MessageType.Name,
//         //                 evt.Output.Count, stopwatch!.Elapsed
//         //             );
//         //             //
//         //             // if (evt.Output.Any()) {
//         //             //     Logger.LogDebug(
//         //             //         "{Event} {RecordId} {MessageType} ({OutputCount}) {Elapsed}",
//         //             //         nameof(InputProcessed), evt.Record, evt.Record.MessageType.Name,
//         //             //         evt.Output.Count, stopwatch!.Elapsed
//         //             //     );
//         //             //     
//         //             //     //
//         //             //     // Logger.LogDebug(
//         //             //     //     "{MessageInfo} processed (output: {OutputCount}) in {ElapsedHumanReadable}",
//         //             //     //     $"{evt.Record.MessageType.Name}(id: {evt.Record})", evt.Output.Count, stopwatch!.GetElapsedHumanReadable(stop: true)
//         //             //     // );
//         //             //     //
//         //             //     // Logger.LogDebug(
//         //             //     //     "message processed (id: {RecordId} type: {MessageType} output: {OutputCount})",
//         //             //     //     evt.Record, evt.Record.MessageType.Name, evt.Output.Count
//         //             //     // );
//         //             // }
//         //             // else {
//         //             //     Logger.LogDebug(
//         //             //         "{Event} {RecordId} {MessageType} {Elapsed}",
//         //             //         nameof(InputProcessed), evt.Record, evt.Record.MessageType.Name, stopwatch!.Elapsed
//         //             //     );
//         //             //     
//         //             //     Logger.LogDebug(
//         //             //         "{RecordId}({MessageType}) processed in {ElapsedHumanReadable}",
//         //             //         evt.Record, evt.Record.MessageType.Name, stopwatch!.GetElapsedHumanReadable(stop: true)
//         //             //     );
//         //             // }
//         //         }
//         //     }
//         // );
//
//         On<InputError>(
//             evt => {
//                 using (Logger.WithRecordInfo(evt.Record))
//                     Logger.LogError(
//                         evt.Exception,
//                         "{RecordId} message {MessageType} processing error: {ErrorMessage}",
//                         evt.Record, evt.Record.MessageType.Name, evt.Exception.Message
//                     );
//             }
//         );
//
//         On<OutputProcessed>(
//             evt => {
//                 using (Logger.WithRecordInfo(evt.Input))
//                     if (evt.Result.RecordPersisted) {
//                         Logger.LogDebug(
//                             "{Event} {RecordId} | {RequestId} >> {OutputRecordId}",
//                             nameof(OutputProcessed), evt.Input, evt.Result.RequestId,
//                             $"{evt.Result.RecordId.Topic}:{evt.Result.RecordId.Partition}@{evt.Result.RecordId.Offset}"
//                         );
//                     }
//                     else {
//                         Logger.LogError(
//                             "{Event} {RecordId} | {RequestId} ## {ErrorMessage}",
//                             nameof(OutputProcessed), evt.Input, evt.Result.RequestId, evt.Result.Exception!.Message
//                         );
//                     }
//             }
//         );
//         
//         // On<InputPositionTracked>(
//         //     evt => {
//         //         log.LogTrace(
//         //             "[{ProcessorName}] {InputId} position tracked {Topic} [{Partition}] @ {Position}",
//         //             evt.Processor, evt.Record.RequestId, 
//         //             evt.Record.Position.Topic, evt.Record.Position.Partition.Value, evt.Record.Position.Offset.Value
//         //         );
//         //     }
//         // );
//         
//         // On<BeforeProduce>(
//         //     evt => {
//         //         Logger.LogDebug(
//         //             "sending {RequestId} {MessageType} ({Key}) >> {Topic}",
//         //             evt.Request.RequestId, evt.Request.Message.GetType().Name, evt.Request.Key, evt.Request.Topic
//         //         );
//         //     }
//         // );
//         //
//         // On<ProduceResultReceived>(
//         //     evt => {
//         //         if (evt.Result.DeliveryFailed) {
//         //             Logger.LogError(
//         //                 evt.Result.Exception,
//         //                 "failed to send {RequestId} :: {ErrorMessage}",
//         //                 evt.Result.RequestId, evt.Result.Exception!.Message
//         //             );
//         //         }
//         //         else {
//         //             // log.Verbose(
//         //             //     "[{ProducerName}] delivered {MessageType} ({Key}) >> {Topic} [{Partition}] @ {Offset} : {RequestId}",
//         //             //     evt.ProducerName, ack.Request.Message.GetType().Name, ack.Request.Key,
//         //             //     ack.Position.Topic, ack.Position.Partition.Value, ack.Position.Offset.Value,
//         //             //     ack.Request.RequestId
//         //             // );
//         //         }
//         //     }
//         // );
//         //
//         // On<ProduceResultUserHandlingError>(
//         //     evt => {
//         //         Logger.LogError(
//         //             evt.UserException,
//         //             "failed handling producer callback for {RequestId} :: {ErrorMessage}",
//         //             evt.Result.RequestId, evt.UserException.Message
//         //         );
//         //     }
//         // );
//         //
//         // On<ProduceResultError>(
//         //     evt => {
//         //         Logger.LogError(
//         //             evt.Exception,
//         //             "{ProducerName} | {RequestId} | failed handling callback: {ErrorMessage}",
//         //             evt.ProducerName, evt.RequestId, evt.Exception.Message
//         //         );
//         //     }
//         // );
//     }
// }