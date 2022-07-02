// // ReSharper disable MethodSupportsCancellation
//
// using System.Collections.Concurrent;
// using System.Runtime.ExceptionServices;
// using Confluent.Kafka;
// using Confluent.SchemaRegistry;
// using Krimson.Interceptors;
// using Krimson.Logging;
// using Krimson.Processors.Configuration;
// using Krimson.Processors.Interceptors;
// using Krimson.Producers;
// using Microsoft.Extensions.Logging;
//
// namespace Krimson.Processors;
//
// public enum KrimsonProcessorStatus {
//     Running,
//     Stopping,
//     Stopped
// }
//
// [PublicAPI]
// public sealed class KrimsonProcessor : IKrimsonProcessor {
//     public static KrimsonProcessorBuilder Builder => new();
//
//     public KrimsonProcessor(KrimsonProcessorOptions options) {
//         ClientId = options.ConsumerConfiguration.ClientId;
//         GroupId  = options.ConsumerConfiguration.GroupId;
//         Router   = options.Router;
//         Topics   = options.InputTopics;
//         Logger   = options.LoggerFactory.CreateLogger(ClientId);
//         Registry = options.RegistryFactory();
//
//         Intercept = options.Interceptors
//             .Prepend(new KrimsonProcessorLogger(){ Name = $"KrimsonProcessor({ClientId})"})
//             .Prepend(new ConfluentProcessorLogger())
//             .WithLoggerFactory(options.LoggerFactory)
//             .Intercept;
//
//         Producer = new KrimsonProducer(
//             options.ProducerConfiguration,
//             Intercept,
//             options.SerializerFactory(Registry),
//             options.OutputTopic?.Name
//         );
//         
//         // All handlers (except the log handler) are executed as a
//         // side-effect of, and on the same thread as the Consume or
//         // Close methods. Any exception thrown in a handler (with
//         // the exception of the log and error handlers) will
//         // be propagated to the application via the initiating
//         // call. i.e. in this example, any exceptions thrown in this
//         // handler will be exposed via the Consume method in the main
//         // consume loop and handled by the try/catch block there.
//         
//         Consumer = new ConsumerBuilder<byte[], object?>(options.ConsumerConfiguration)
//             .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
//             .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
//             .SetValueDeserializer(options.DeserializerFactory(Registry))
//             .SetPartitionsAssignedHandler((_, partitions) => Intercept(new PartitionsAssigned(ClientId, partitions)))
//             .SetOffsetsCommittedHandler((_, committed) => Intercept(new PositionsCommitted(ClientId, committed.Offsets, committed.Error)))
//             .SetPartitionsRevokedHandler(
//                 (_, positions) => {
//                     Intercept(new PartitionsRevoked(ClientId, positions));
//                     Flush(positions);
//                 }
//             )
//             .SetPartitionsLostHandler(
//                 (_, positions) => {
//                     Intercept(new PartitionsLost(ClientId, positions));
//                     Flush(positions);
//                 }
//             )
//             .Build();
//
//         Status = KrimsonProcessorStatus.Stopped;
//     }
//
//     ILogger                    Logger    { get; }
//     ISchemaRegistryClient      Registry  { get; }
//     IConsumer<byte[], object?> Consumer  { get; }
//     KrimsonProducer            Producer  { get; }
//     Intercept                  Intercept { get; }
//     KrimsonProcessorRouter     Router    { get; }
//
//     CancellationTokenSource Cancellator { get; set; } = null!;
//     OnProcessorStop         OnStop      { get; set; } = null!;
//
//     public string                 ClientId { get; }
//     public string                 GroupId  { get; }
//     public string[]               Topics   { get; }
//     public KrimsonProcessorStatus Status   { get; private set; }
//     
//     public async Task Start(CancellationToken stoppingToken, OnProcessorStop? onStop = null) {
//         if (Status == KrimsonProcessorStatus.Running)
//             return;
//
//         Consumer.Subscribe(Topics);
//
//         Status      = KrimsonProcessorStatus.Running;
//         OnStop      = onStop ?? ((_, _) => Task.CompletedTask);
//         Cancellator = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
//
//         Cancellator.Token.Register(() => Stop().GetAwaiter().GetResult());
//
//         await Task.Yield();
//
//         Intercept(new ProcessorStarted(ClientId, GroupId, Topics));
//         
//         try {
//             await foreach (var record in Consumer.Records(position => Intercept(new PartitionEndReached(ClientId, position)), Cancellator.Token)) {
//                 await ProcessRecord(record, Cancellator.Token);
//             }
//         }
//         catch (Exception ex) {
//             await Stop(ExceptionDispatchInfo.Capture(ex));
//         }
//         // finally{
//         //     if (Status == KrimsonProcessorStatus.Running)
//         //         await Stop();
//         // }
//     }
//
//     public async ValueTask DisposeAsync() {
//         if (Status == KrimsonProcessorStatus.Running)
//             await Stop().ConfigureAwait(false);
//     }
//
//     public Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGap() => Consumer.GetSubscriptionGap();
//
//     public Task Stop() => Stop(null);
//     
//     async Task Stop(ExceptionDispatchInfo? capturedException) {
//         if (Status != KrimsonProcessorStatus.Running) {
//             Intercept(
//                 new ProcessorStopped(
//                     ClientId, GroupId, Topics,
//                     new($"{ClientId} already {Status.ToString().ToLower()}. This should not happen! Investigate!", capturedException?.SourceException)
//                 )
//             );
//             
//             return;
//         }
//
//         if (capturedException?.SourceException is OperationCanceledException)
//             capturedException = null;
//
//         Status = KrimsonProcessorStatus.Stopping;
//         
//         Intercept(new ProcessorStopping(ClientId, GroupId, Topics));
//         
//         if (!Cancellator.IsCancellationRequested)
//             Cancellator.Cancel(); // check side effects
//         
//         IReadOnlyCollection<SubscriptionTopicGap> gap = Array.Empty<SubscriptionTopicGap>();
//
//         try {
//             gap = await Consumer
//                 .Stop()
//                 .ConfigureAwait(false);
//             
//             Consumer.Dispose();
//
//             await Producer
//                 .DisposeAsync()
//                 .ConfigureAwait(false);
//
//             Registry.Dispose();
//         }
//         catch (Exception disposeEx) {
//             disposeEx = new Exception($"{ClientId} stopped suddenly!", disposeEx);
//             capturedException = capturedException is not null
//                 ? ExceptionDispatchInfo.Capture(new AggregateException(capturedException.SourceException!, disposeEx).Flatten())
//                 : ExceptionDispatchInfo.Capture(disposeEx);
//         }
//
//         Status = KrimsonProcessorStatus.Stopped;
//             
//         Intercept(new ProcessorStopped(ClientId, GroupId, Topics, capturedException?.SourceException));
//
//         try {
//             await OnStop(gap, capturedException?.SourceException)
//                 .ConfigureAwait(false);
//         }
//         catch (Exception ex) {
//              Intercept(new ProcessorStoppedUserHandlingError(ClientId, GroupId, Topics, ex));
//         }
//     }
//
//     void Flush(List<TopicPartitionOffset> positions) {
//         Producer.Flush();
//         Consumer.CommitAll();
//     }
//
//     async Task ProcessRecord(KrimsonRecord record, CancellationToken cancellationToken) {
//         if (!Router.CanRoute(record)) {
//             Consumer.TrackPosition(record);
//             Intercept(new InputSkipped(ClientId, record));
//             return;
//         }
//
//         Intercept(new InputReady(ClientId, record));
//
//         var context = new KrimsonProcessorContext(record, Logger, cancellationToken);
//         
//         try {
//
//             using (Logger.WithRecordInfo(record)) {
//                 await Router
//                     .Process(context)
//                     .ConfigureAwait(false);
//             }
//             
//             Intercept(new InputConsumed(ClientId, record, context.GeneratedOutput()));
//         }
//         catch (Exception ex) {
//             Intercept(new InputError(ClientId, record, ex));
//             throw;
//         }
//         
//         ProcessOutput(context.GeneratedOutput());
//
//         void ProcessOutput(IReadOnlyCollection<ProducerRequest> requests) {
//             if (requests.Count == 0) {
//                 Consumer.TrackPosition(record);
//                 Intercept(new InputProcessed(ClientId, record, requests));
//                 return;
//             }
//
//             var results = new ConcurrentQueue<ProducerResult>();
//
//             foreach (var request in requests)
//                 Producer.Produce(request, result => OnResult(request, result));
//
//             void OnResult(ProducerRequest message, ProducerResult result) { 
//                 Intercept(new OutputProcessed(ClientId, Producer.ClientId, result, record, message));
//
//                 if (result.Success) {
//                     results.Enqueue(result);
//
//                     if (results.Count < requests.Count)
//                         return;
//
//                     Consumer.TrackPosition(record);
//
//                     Intercept(new InputProcessed(ClientId, record, requests));
//                 }
//                 else {
//                     // dont wait here, just let it flow...
//                     Stop(ExceptionDispatchInfo.Capture(result.Exception!));
//                 }
//             }
//         }
//
// //         void ProcessOutput(IReadOnlyCollection<ProducerRequest> messages) {
// //             var messageCount = messages.Count;
// //
// //             if (messageCount == 0) {
// //                 Consumer.TrackPosition(record);
// //                 Intercept(new InputProcessed(ClientId, record, messages));
// //                 return;
// //             }
// //
// //             var results       = new ConcurrentQueue<ProducerResult>();
// //             var alreadyFailed = false; //TODO SS: should not happen anymore... check it out later
// //
// //             foreach (var message in messages)
// //                 Producer.Produce(
// //                     message, result => {
// //                         Intercept(
// //                             new OutputProcessed(
// //                                 ClientId, Producer.ClientId, result,
// //                                 record, message
// //                             )
// //                         );
// //
// //                         if (result.Success) {
// //                             results.Enqueue(result);
// //
// //                             if (results.Count != messageCount)
// //                                 return;
// //
// //                             Consumer.TrackPosition(record);
// //
// //                             Intercept(new InputProcessed(ClientId, record, messages));
// //                         }
// //                         else {
// //                             if (alreadyFailed)
// //                                 return;
// //
// //                             alreadyFailed = true;
// //
// //                             // dont wait here, just let it flow...
// // #pragma warning disable CS4014
// //                             Stop(ExceptionDispatchInfo.Capture(result.Exception!));
// // #pragma warning restore CS4014
// //                         }
// //                     }
// //                 );
// //         }
//
//     }
// }