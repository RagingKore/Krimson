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

    static readonly TimeSpan DisposeTimeout      = TimeSpan.FromSeconds(90);
    static readonly TimeSpan FlushTimeout        = TimeSpan.FromSeconds(60);
    static readonly TimeSpan TransientErrorDelay = TimeSpan.FromSeconds(1);

    // static ManualResetEventSlim Gatekeeper { get; } = new(true);
    
    public KrimsonProducer(
        ProducerConfig configuration,
        Intercept intercept,
        IDynamicSerializer serializer,
        string? defaultTopic = null
    ) {
        ProducerName = configuration.ClientId;
        Intercept    = intercept;
        Topic        = defaultTopic;
        
        Client = new ProducerBuilder<byte[], object?>(configuration)
            .SetValueSerializer(serializer)
            .SetErrorHandler((_, err) => Intercept(new OnConfluentProducerError(ProducerName, new KafkaException(err))))
            .Build();

        InFlightMessageCounter = new();
    }
    
    protected internal IProducer<byte[], object?> Client { get; }

    Intercept              Intercept              { get; }
    InFlightMessageCounter InFlightMessageCounter { get; }

    public string  ProducerName { get; }
    public string? Topic        { get; }

    public long InFlightMessages => InFlightMessageCounter;

    public void Produce(ProducerRequest request,  Action<ProducerResult> onResponse) {
        Ensure.NotNull(request, nameof(request));
        Ensure.NotNull(onResponse, nameof(onResponse));

        // Gatekeeper.Wait(); // might be flushing...

        InFlightMessageCounter.Increment();
        
        // try to set default topic if missing
        request = request.Topic switch {
            null when Topic is null => throw new("Please provide a destination topic."),
            null                    => request with { Topic = Topic },
            _                       => request
        };

        Intercept(new OnProduce(ProducerName, request));
        
        while (true) {
            try {
                Client.Produce(
                    request.Topic, CreateDeserializedMessage(request, ProducerName), report => {
                        InFlightMessageCounter.Decrement();

                        var result = ProducerResult.From(request.RequestId, report);

                        Intercept(new OnProduceResult(ProducerName, result));

                        try { 
                            onResponse(result); 
                        }
                        catch (Exception userException) {
                            Intercept(new OnProduceResultUserException(ProducerName, result, userException));
                        }
                    }
                );
            }
            catch (KafkaException ex) {
                if (ex.Error.IsFatal) {
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
                    if (result.DeliveryFailed) {
                        if (throwOnError)
                            tcs.TrySetException(result.Exception!);
                        else
                            tcs.TrySetResult(result);
                    }
                    else
                        tcs.TrySetResult(result);
                }
            );
        }
        catch (Exception ex) {
            tcs.TrySetException(ex);
        }

        return await tcs.Task;
    }

    /// <summary>
    /// Forcefully blocks sends until it has finished flushing and attempts to use internal in-flight count
    /// to ensure that absolutely all messages were flushed.
    /// </summary>
    public ValueTask Flush(CancellationToken cancellationToken = default) {
        while (true) {
            try {
                var pending = Client.Flush(FlushTimeout);

                if (pending == 0 && InFlightMessageCounter == 0)
                    break;
                
                if (cancellationToken.IsCancellationRequested)
                    break;
            }
            catch (KafkaException kex) {
                if (kex.Error.IsFatal)
                    throw;
            }

            Client.Poll(TransientErrorDelay);
        }
        
        return ValueTask.CompletedTask;
    }

    public virtual async ValueTask DisposeAsync() {
        // let's cancel after 90 seconds as a failsafe
        // since by sometimes and by some unknown reason
        // disposing actually locks forever
        using var cancellator = new CancellationTokenSource(DisposeTimeout);

        await Flush(cancellator.Token).ConfigureAwait(false);
        await Task.Run(() => Client.Dispose(), cancellator.Token);
    }

    public static Message<byte[], object?> CreateDeserializedMessage(ProducerRequest request, string producerName) {
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