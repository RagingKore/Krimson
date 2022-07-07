using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Producers.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.Serializers;
using static System.String;

namespace Krimson.Producers;

class InFlightMessageCounter {
    long _count;

    long Count => Interlocked.Read(ref _count);

    public long Increment() => Interlocked.Increment(ref _count);
    public long Decrement() => Interlocked.Decrement(ref _count);
    
    public static implicit operator long(InFlightMessageCounter self) => self.Count;
}

[PublicAPI]
public class KrimsonProducer : IAsyncDisposable {
    public static KrimsonProducerBuilder Builder => new();

    static readonly TimeSpan DisposeTimeout      = TimeSpan.FromSeconds(120);
    static readonly TimeSpan FlushTimeout        = TimeSpan.FromMilliseconds(250);
    static readonly TimeSpan TransientErrorDelay = TimeSpan.FromSeconds(1);
    
    static readonly KafkaException DestinationTopicRequiredException = new KafkaException(
        new Error(ErrorCode.TopicException, "Please provide a destination topic")
    );
    
    // static ManualResetEventSlim Gatekeeper { get; } = new(true);
    
    public KrimsonProducer(
        ProducerConfig configuration,
        Intercept intercept,
        IDynamicSerializer serializer,
        string? defaultTopic = null
    ) {
        ClientId  = configuration.ClientId;
        Intercept = intercept;
        Topic     = defaultTopic;
        
        // // ReSharper disable once SuspiciousTypeConversion.Global
        // if (serializer is ISerializer<byte[]> bytesSerializer) {
        //     var Client = new ProducerBuilder<byte[], byte[]>(configuration)
        //         .SetValueSerializer(Serializers.ByteArray)
        //         .SetLogHandler((pdr, log) => Intercept(new ConfluentProducerLog(ClientId, pdr.GetInstanceName(), log)))
        //         .SetErrorHandler((pdr, err) => Intercept(new ConfluentProducerError(ClientId, pdr.GetInstanceName(), err)))
        //         .Build();
        // }
        
        Client = new ProducerBuilder<byte[], object?>(configuration)
            .SetValueSerializer(serializer)
            .SetLogHandler((pdr, log) => Intercept(new ConfluentProducerLog(ClientId, pdr.GetInstanceName(), log)))
            .SetErrorHandler((pdr, err) => Intercept(new ConfluentProducerError(ClientId, pdr.GetInstanceName(), err)))
            .Build();

        InFlightMessageCounter = new();
    }
    
    protected internal IProducer<byte[], object?> Client { get; }

    Intercept              Intercept              { get; }
    InFlightMessageCounter InFlightMessageCounter { get; }

    public string  ClientId { get; }
    public string? Topic    { get; }

    public long InFlightMessages => InFlightMessageCounter;

    public void Produce(ProducerRequest request,  Action<ProducerResult> onResult) {
        Ensure.NotNull(request = EnsureTopicIsSet(request, Topic), nameof(request));
        Ensure.NotNull(onResult, nameof(onResult));

        InFlightMessageCounter.Increment();

        Intercept(new BeforeProduce(ClientId, request));
        
        while (true) {
            try {
                Client.Produce(request.Topic, CreateKafkaMessage(request, ClientId), DeliveryReportHandler());
                break;
            }
            catch (KafkaException kex) when (kex.IsTransient()) {
                // An immediate failure of the produce call is most often caused by the
                // local message queue being full, and appropriate response to that is
                // to retry. (ErrorCode.Local_QueueFull)
                Client.Poll(TransientErrorDelay);
            }
            catch (KafkaException kex) when (kex.IsTerminal()) {
                InFlightMessageCounter.Decrement();
                throw;
            }
        }

        static ProducerRequest EnsureTopicIsSet(ProducerRequest request, string? defaultTopic) {
            return IsNullOrWhiteSpace(request.Topic) switch {
                false                              => request,
                true when defaultTopic is not null => request with { Topic = defaultTopic },
                _                                  => throw DestinationTopicRequiredException
            };
        }

        Action<DeliveryReport<byte[], object?>> DeliveryReportHandler() =>
            report => {
                try {
                    var result = ProducerResult.From(request.RequestId, report);

                    Intercept(new ProduceResultReceived(ClientId, result));

                    try {
                        onResult(result);
                    }
                    catch (Exception uex) {
                        Intercept(new ProduceResultUserHandlingError(ClientId, result, uex));
                    }

                    InFlightMessageCounter.Decrement();
                }
                catch (Exception ex) {
                    Intercept(new ProduceResultError(ClientId, request.RequestId, report, ex));
                }
            };
    }

    public void Produce(ProducerRequest request, Func<ProducerResult, Task> onResult) =>
        Produce(request, result => onResult(result).Synchronously());
    
    public Task<ProducerResult> Produce(ProducerRequest request, bool throwOnError = true) {
        Ensure.NotNull(request, nameof(request));

        var tcs = new TaskCompletionSource<ProducerResult>(TaskCreationOptions.AttachedToParent);

        try {
            Produce(
                request,
                result => {
                    if (result.Success)
                        tcs.TrySetResult(result);
                    else {
                        if (throwOnError)
                            tcs.TrySetException(result.Exception!);
                        else
                            tcs.TrySetResult(result);
                    }
                }
            );
        }
        catch (Exception ex) {
            tcs.TrySetException(ex);
        }

        return tcs.Task;
    }
    
    public long Flush(CancellationToken cancellationToken = default) {
        do {
            try {
                var pending = Client.Flush(FlushTimeout);

                if (pending == 0 && InFlightMessageCounter == 0)
                    break;
            }
            catch (OperationCanceledException) {
               break;
            }
            catch (KafkaException kex) when (kex.IsTransient()) {
                Client.Poll(TransientErrorDelay);
            }
        } while (!cancellationToken.IsCancellationRequested);

        return InFlightMessageCounter;
    }

    public virtual async ValueTask DisposeAsync() {
        // let's cancel after a default timeout as a failsafe
        // since sometimes and by unknown reasons disposing
        // actually locks forever. must investigate further...
        using var cancellator = new CancellationTokenSource(DisposeTimeout);

        await Task.Run(() => Flush(cancellator.Token), cancellator.Token);
        await Task.Run(() => Client.Dispose(), cancellator.Token);
    }

    public static Message<byte[], object?> CreateKafkaMessage(ProducerRequest request, string producerName) {
        request.Headers[HeaderKeys.ProducerName] = producerName;
        request.Headers[HeaderKeys.RequestId]    = request.RequestId.ToString();

        return new Message<byte[], object?> {
            Value     = request.Message,
            Headers   = request.Headers.Encode(),
            Timestamp = new Timestamp(DateTime.UtcNow)
        }.With(x => x.Key = request.Key, when: request.HasKey);
    }
}


public static class KrimsonProducerExtensions {
    public static Task<ProducerResult> Produce(this KrimsonProducer producer, object message, MessageKey key) {
        var req = ProducerRequest.Builder
            .Message(message)
            .Key(key)
            .Create();
        
        return producer.Produce(req);
    }
    
    public static Task<ProducerResult> Produce(this KrimsonProducer producer, object message, MessageKey key, string topic) {
        var req = ProducerRequest.Builder
            .Message(message)
            .Key(key)
            .Topic(topic)
            .Create();
        
        return producer.Produce(req);
    }
    
    public static void Produce(this KrimsonProducer producer, object message, MessageKey key, Action<ProducerResult> onResult) {
        var req = ProducerRequest.Builder
            .Message(message)
            .Key(key)
            .Create();
        
        producer.Produce(req, onResult);
    }
    
    public static void Produce(this KrimsonProducer producer, object message, MessageKey key, string topic, Action<ProducerResult> onResult) {
        var req = ProducerRequest.Builder
            .Message(message)
            .Key(key)
            .Topic(topic)
            .Create();
        
        producer.Produce(req, onResult);
    }
}