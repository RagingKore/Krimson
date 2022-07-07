using Krimson.Producers;
using Serilog;

namespace Krimson.Processors;

[PublicAPI]
public class KrimsonProcessorContext {
    public KrimsonProcessorContext(KrimsonRecord record, ILogger logger, CancellationToken cancellationToken) {
        Record            = record;
        Logger            = logger;
        CancellationToken = cancellationToken;
        OutputMessages    = new Queue<ProducerRequest>();
        QueueLocked       = new InterlockedBoolean();
    }

    public KrimsonRecord     Record            { get; }
    public ILogger           Logger            { get; }
    public CancellationToken CancellationToken { get; }

    Queue<ProducerRequest> OutputMessages { get; }
    InterlockedBoolean     QueueLocked    { get; }

    /// <summary>
    /// Enqueues message to be sent on exit
    /// </summary>
    public void Output(ProducerRequest request) {
        if (QueueLocked.CurrentValue)
            throw new InvalidOperationException("Message already processed. Make sure any async operations are awaited.");

        OutputMessages.Enqueue(request);
    }
    
    /// <summary>
    /// Enqueues message to be sent on exit
    /// </summary>
    public void Output(ProducerRequestBuilder builder) => 
        Output(builder.Create());

    /// <summary>
    /// Enqueues message to be sent on exit
    /// </summary>
    public void Output(Func<ProducerRequestBuilder, ProducerRequestBuilder> build) => 
        Output(ProducerRequest.Builder.With(build));

    /// <summary>
    /// Enqueues message to be sent on exit
    /// </summary>
    public void Output(object message) => 
        Output(x => x.Message(message));
    
    /// <summary>
    /// Enqueues message to be sent on exit
    /// </summary>
    public void Output(object message, MessageKey key) => 
        Output(x => x.Message(message).Key(key));

    /// <summary>
    /// Enqueues message to be sent on exit
    /// </summary>
    public void Output(object message, MessageKey key, string topic) => 
        Output(x => x.Message(message).Key(key).Topic(topic));

    public IReadOnlyCollection<ProducerRequest> GeneratedOutput() {
        QueueLocked.EnsureCalledOnce();
        return OutputMessages;
    }
}