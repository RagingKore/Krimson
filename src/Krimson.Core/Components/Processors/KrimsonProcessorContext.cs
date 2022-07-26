using Krimson.Producers;
using Serilog;

namespace Krimson.Processors;

[PublicAPI]
public class KrimsonProcessorContext {
    public KrimsonProcessorContext(KrimsonRecord record, ILogger logger, CancellationToken cancellationToken) {
        Record            = record;
        Logger            = logger;
        CancellationToken = cancellationToken;
        MessageQueue      = new Queue<ProducerRequest>();
        QueueLocked       = new InterlockedBoolean();
    }

    public KrimsonRecord     Record            { get; }
    public ILogger           Logger            { get; }
    public CancellationToken CancellationToken { get; }

    Queue<ProducerRequest> MessageQueue { get; }
    InterlockedBoolean     QueueLocked  { get; }

    /// <summary>
    /// Enqueues message to be sent on exit
    /// </summary>
    public void Output(ProducerRequest request) {
        if (QueueLocked.CurrentValue)
            throw new InvalidOperationException("Messages already processed. Make sure any async operations are awaited.");

        MessageQueue.Enqueue(request);
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

    public IReadOnlyCollection<ProducerRequest> OutputMessages(bool clear = false) {
        QueueLocked.EnsureCalledOnce();

        var messages = MessageQueue.ToArray();
        
        if (clear) MessageQueue.Clear();
        
        return messages;
    }

    public void ClearOutput() => MessageQueue.Clear();
}