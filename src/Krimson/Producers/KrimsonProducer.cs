using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Producers.Interceptors;
using Krimson.SchemaRegistry;

namespace Krimson.Producers;

class InFlightMessageCounter {
    long _count;

    public long Count => Interlocked.Read(ref _count);

    public long Increment() => Interlocked.Increment(ref _count);
    public long Decrement() => Interlocked.Decrement(ref _count);
    
    public static implicit operator long(InFlightMessageCounter self) => self.Count;
}

[PublicAPI]
public class KrimsonProducer : IAsyncDisposable {
    public static KrimsonProducerBuilder Builder => new();

    static readonly TimeSpan DisposeTimeout      = TimeSpan.FromSeconds(120);
    static readonly TimeSpan FlushTimeout        = TimeSpan.FromSeconds(90);
    static readonly TimeSpan TransientErrorDelay = TimeSpan.FromSeconds(1);

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
        Ensure.NotNull(request, nameof(request));
        Ensure.NotNull(onResult, nameof(onResult));

        // Gatekeeper.Wait(); // might be flushing...

        InFlightMessageCounter.Increment();
        
        // try to set default topic if missing
        request = EnsureTopicIsSet(request, Topic);

        Intercept(new BeforeProduce(ClientId, request));
        
        while (true) {
            try {
                var message = CreateKafkaMessage(request, ClientId);
                Client.Produce(request.Topic, message, DeliveryHandler());
            }
            // catch (ProduceException<byte[], object?> pex) {
            //     if (pex.IsTerminal()) {
            //         InFlightMessageCounter.Decrement();
            //         throw;
            //     }
            //
            //     // An immediate failure of the produce call is most often caused by the
            //     // local message queue being full, and appropriate response to that is
            //     // to retry. (ErrorCode.Local_QueueFull)
            //
            //     Client.Poll(TransientErrorDelay);
            //
            //     continue;
            // }
            catch (KafkaException kex) {
                if (kex.IsTerminal()) {
                    InFlightMessageCounter.Decrement();
                    throw;
                }

                // An immediate failure of the produce call is most often caused by the
                // local message queue being full, and appropriate response to that is
                // to retry. (ErrorCode.Local_QueueFull)

                Client.Poll(TransientErrorDelay);

                continue;
            }

            break;
        }

        Action<DeliveryReport<byte[], object?>> DeliveryHandler() =>
            report => {
                try {
                    var result = ProducerResult.From(request.RequestId, report);

                    Intercept(new ProducerResultReceived(ClientId, result));

                    try {
                        onResult(result);
                    }
                    catch (Exception uex) {
                        Intercept(new ProducerResultError(ClientId, request.RequestId, report, uex));
                    }

                    InFlightMessageCounter.Decrement();
                }
                catch (Exception ex) {
                    Intercept(new ProducerResultError(ClientId, request.RequestId, report, ex));
                }
            };
        
        // Action<DeliveryReport<byte[], object?>> DeliveryHandler(ProducerRequest producerRequest, Action<ProducerResult> onUserResultHandler) =>
        //     report => {
        //         try {
        //             InFlightMessageCounter.Decrement();
        //
        //             var result = ProducerResult.From(producerRequest.RequestId, report);
        //
        //             Intercept(new OnProduceResult(ClientId, result));
        //
        //             try {
        //                 onUserResultHandler(result);
        //             }
        //             catch (Exception userException) {
        //                 Intercept(new OnProduceResultUserException(ClientId, result, userException));
        //             }
        //         }
        //         catch (Exception ex) {
        //             Intercept(new OnProduceResultException(ClientId, report, ex));
        //         }
        //     };
        static ProducerRequest EnsureTopicIsSet(ProducerRequest request, string? defaultTopic) {
            return request.Topic switch {
                null when defaultTopic is null => throw new("Please provide a destination topic."),
                null => request with {
                    Topic = defaultTopic
                },
                _ => request
            };
        }
    }
    
    public void Produce(ProducerRequest request, Func<ProducerResult, Task> onResult) =>
        Produce(request, result => onResult(result).Synchronously());
    
    public async Task<ProducerResult> Produce(ProducerRequest request, bool throwOnError = true) {
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

        return await tcs.Task;
    }
    
    // public ValueTask Flush(CancellationToken cancellationToken = default) {
    //     while (true) {
    //         try {
    //             var pending = Client.Flush(FlushTimeout);
    //
    //             if (pending == 0 && InFlightMessageCounter == 0)
    //                 break;
    //             
    //             if (cancellationToken.IsCancellationRequested)
    //                 break;
    //         }
    //         catch (KafkaException kex) {
    //             if (kex.Error.IsFatal)
    //                 throw;
    //         }
    //
    //         Client.Poll(TransientErrorDelay);
    //     }
    //     
    //     return ValueTask.CompletedTask;
    // }

    public void Flush(CancellationToken cancellationToken = default) {
        while (true) {
            try {
                var pending = Client.Flush(FlushTimeout);

                if (pending == 0 && InFlightMessageCounter == 0)
                    break;

                if (cancellationToken.IsCancellationRequested)
                    break;
            }
            catch (KafkaException kex) when (!kex.IsTerminal()) {
                var temp_wtf = kex.Error;
            }

            Client.Poll(TransientErrorDelay);
        }
    }
    
    public virtual async ValueTask DisposeAsync() {
        // let's cancel after 90 seconds as a failsafe
        // since by sometimes and by some unknown reason
        // disposing actually locks forever
        using var cancellator = new CancellationTokenSource(DisposeTimeout);

        // await Flush(cancellator.Token).ConfigureAwait(false);
        await Task.Run(() => Flush(cancellator.Token));
        await Task.Run(() => Client.Dispose(), cancellator.Token);
    }

    public static Message<byte[], object?> CreateKafkaMessage(ProducerRequest request, string producerName) {
        // enrich
        request.Headers[HeaderKeys.ProducerName] = producerName;
        request.Headers[HeaderKeys.RequestId]    = request.RequestId.ToString();

        return new Message<byte[], object?> {
            Value     = request.Message,
            Headers   = request.Headers.Encode(),
            Timestamp = new(DateTime.UtcNow)
        }.With(x => x.Key = request.Key, request.HasKey);
    }
}