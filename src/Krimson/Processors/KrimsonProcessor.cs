// ReSharper disable MethodSupportsCancellation

using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.Processors.Configuration;
using Krimson.Processors.Interceptors;
using Krimson.Producers;
using Serilog.Enrichers;

namespace Krimson.Processors;

public enum KrimsonProcessorStatus {
    Running,
    Stopping,
    Stopped
}

[PublicAPI]
public sealed class KrimsonProcessor : IKrimsonProcessor {
    public static KrimsonProcessorBuilder Builder => new();

    public KrimsonProcessor(KrimsonProcessorOptions options) {
        ProcessorName    = options.ConsumerConfiguration.ClientId;
        SubscriptionName = options.ConsumerConfiguration.GroupId;
        Router           = options.Router;
        Topics           = options.InputTopics;
        
        Log = Serilog.Log
            .ForContext("SourceContext", ProcessorName)
            .ForContext(nameof(SubscriptionName), SubscriptionName);

        Intercept = new InterceptorCollection(
            new[] {
                new LoggingProcessorInterceptor(Log)
            }.Concat(options.Interceptors)
        ).Intercept;

        Registry = options.RegistryFactory();

        Producer = new KrimsonProducer(
            options.ProducerConfiguration,
            options.Interceptors.Intercept,
            options.SerializerFactory(Registry),
            options.OutputTopic?.Name
        );
        
        // All handlers (except the log handler) are executed as a
        // side-effect of, and on the same thread as the Consume or
        // Close methods. Any exception thrown in a handler (with
        // the exception of the log and error handlers) will
        // be propagated to the application via the initiating
        // call. i.e. in this example, any exceptions thrown in this
        // handler will be exposed via the Consume method in the main
        // consume loop and handled by the try/catch block there.
        
        Consumer = new ConsumerBuilder<byte[], object?>(options.ConsumerConfiguration)
            //.SetLogHandler()
            .SetErrorHandler((_, error) => Intercept(new ConfluentConsumerError(ProcessorName, new KafkaException(error))))
            .SetValueDeserializer(options.DeserializerFactory(Registry))
            .SetPartitionsAssignedHandler((_, partitions) => Intercept(new PartitionsAssigned(ProcessorName, partitions)))
            .SetOffsetsCommittedHandler((_, committed) => Intercept(new PositionsCommitted(ProcessorName, committed.Offsets, new KafkaException(committed.Error))))
            .SetPartitionsRevokedHandler(
                (_, positions) => {
                    Intercept(new PartitionsRevoked(ProcessorName, positions));
                    Flush(positions).Synchronously();
                }
            )
            .SetPartitionsLostHandler(
                (_, positions) => {
                    Intercept(new PartitionsLost(ProcessorName, positions));
                    Flush(positions).Synchronously();
                }
            )
            .Build();

        Status = KrimsonProcessorStatus.Stopped;
    }

    ILogger                    Log       { get; }
    ISchemaRegistryClient      Registry  { get; }
    IConsumer<byte[], object?> Consumer  { get; }
    KrimsonProducer            Producer  { get; }
    Intercept                  Intercept { get; }
    KrimsonProcessorRouter     Router    { get; }

    CancellationTokenSource Cancellator { get; set; } = null!;
    OnProcessorStop         OnStop      { get; set; } = null!;

    public string                 ProcessorName    { get; }
    public string                 SubscriptionName { get; }
    public string[]               Topics           { get; }
    public KrimsonProcessorStatus Status           { get; private set; }
    
    public async Task Start(CancellationToken stoppingToken, OnProcessorStop? onStop = null) {
        if (Status == KrimsonProcessorStatus.Running)
            return;

        Consumer.Subscribe(Topics);
        
        Status      = KrimsonProcessorStatus.Running;
        Cancellator = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        OnStop      = onStop ?? ((_, _) => Task.CompletedTask);

        await Task.Yield();

        Intercept(new ProcessorStarted(ProcessorName, SubscriptionName, Topics));
        
        try {
            await foreach (var record in Consumer.Records(Cancellator.Token)) {
                await ProcessRecord(record, Cancellator.Token);
            }
        }
        catch (Exception ex) {
            await Stop(ExceptionDispatchInfo.Capture(ex));
        }
        finally{
            if (Status == KrimsonProcessorStatus.Running)
                await Stop();
        }
    }

    public async ValueTask DisposeAsync() {
        if (Status == KrimsonProcessorStatus.Running)
            await Stop().ConfigureAwait(false);
    }

    public Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGap() => Consumer.GetSubscriptionGap();

    public Task Stop() => Stop(null);
    
    async Task Stop(ExceptionDispatchInfo? capturedException) {
        if (Status != KrimsonProcessorStatus.Running) {
            Intercept(
                new ProcessorStopped(
                    ProcessorName, SubscriptionName, Topics,
                    new($"{ProcessorName} already {Status.ToString().ToLower()}. This should not happen! Investigate!", capturedException.SourceException)
                )
            );
            
            return;
        }

        if (capturedException?.SourceException is OperationCanceledException)
            capturedException = null;

        Status = KrimsonProcessorStatus.Stopping;
        
        Intercept(new ProcessorStopping(ProcessorName, SubscriptionName, Topics));
        
        if (!Cancellator.IsCancellationRequested)
            Cancellator.Cancel(); // check side effects
        
        IReadOnlyCollection<SubscriptionTopicGap> gap = Array.Empty<SubscriptionTopicGap>();

        try {
            gap = await Consumer
                .Stop()
                .ConfigureAwait(false);
            
            Consumer.Dispose();

            await Producer
                .DisposeAsync()
                .ConfigureAwait(false);
        }
        catch (Exception disposeEx) {
            disposeEx = new Exception($"{ProcessorName} stopped suddenly!", disposeEx);
            capturedException = capturedException is not null
                ? ExceptionDispatchInfo.Capture(new AggregateException(capturedException.SourceException!, disposeEx).Flatten())
                : ExceptionDispatchInfo.Capture(disposeEx);
        }

        Status = KrimsonProcessorStatus.Stopped;
            
        Intercept(new ProcessorStopped(ProcessorName, SubscriptionName, Topics, capturedException?.SourceException));

        try {
            await OnStop(gap, capturedException?.SourceException)
                .ConfigureAwait(false);
        }
        catch (Exception ex) {
            Log.Warning(
                ex,
                "failed to execute {OnProcessorStop}",
                nameof(OnProcessorStop)
            );
        }
    }
    
    /// <summary>
    /// It's like a fake transaction until I actually implement transactions (again ffs)
    /// </summary>
    async Task Flush(List<TopicPartitionOffset> positions) {
        try {
            await Producer.Flush().ConfigureAwait(false);
        }
        catch (Exception ex) {
            throw;
        }

        try {
            Consumer.Commit();
        }
        //catch (KafkaException kex) when (kex.Error.Code != ErrorCode.Local_NoOffset)
        catch (KafkaException ex) {
            if (ex.Error.IsFatal)
                throw;

            if (ex.Error.Code != ErrorCode.Local_NoOffset) {
                Log.Information(ex, "Non fatal consumer commit error!");
            }
        }
        // might not trigger the commit event...
    }

    async Task ProcessRecord(KrimsonRecord record, CancellationToken cancellationToken) {
        if (!Router.CanRoute(record)) {
            Consumer.TrackPosition(record);
            Intercept(new InputSkipped(ProcessorName, record));
            return;
        }

        Intercept(new InputReady(ProcessorName, record));
        
        var context = new KrimsonProcessorContext(record, Log.WithRecordInfo(record), cancellationToken);

        try {
            await Router
                .Process(context)
                .ConfigureAwait(false);

            Intercept(new InputConsumed(ProcessorName, record, context.GeneratedOutput()));
        }
        catch (Exception ex) {
            Intercept(new InputError(ProcessorName, record, ex));
            throw;
        }
        
        ProcessOutput(context.GeneratedOutput());

        void ProcessOutput(IReadOnlyCollection<ProducerRequest> messages) {
            var messageCount = messages.Count;

            if (messageCount == 0) {
                Consumer.TrackPosition(record);
                Intercept(new InputProcessed(ProcessorName, record, messages));
                return;
            }

            var results       = new ConcurrentQueue<ProducerResult>();
            var alreadyFailed = false;

            foreach (var message in messages)
                Producer.Produce(
                    message, result => {
                        if (alreadyFailed) return;

                        if (result.RecordPersisted) {
                            results.Enqueue(result);

                            Intercept(new OutputProcessed(ProcessorName, Producer.ProducerName, result, record));

                            if (results.Count != messageCount)
                                return;

                            Consumer.TrackPosition(record);
                            
                            Intercept(new InputProcessed(ProcessorName, record, messages));
                        }
                        else {
                            alreadyFailed = true;
                            Stop(ExceptionDispatchInfo.Capture(result.Exception!)).Synchronously();
                        }
                    }
                );
        }
    }
}