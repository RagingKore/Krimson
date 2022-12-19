using Krimson.Producers;
using Krimson.State;

namespace Krimson.Processors;

public delegate Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGaps();

[PublicAPI]
public class KrimsonProcessorContext {
    public KrimsonProcessorContext(KrimsonRecord record, ILogger logger, IStateStore state, GetSubscriptionGaps getSubscriptionGaps, CancellationToken cancellationToken) {
        Record              = record;
        Logger              = logger;
        State               = state;
        GetSubscriptionGaps = getSubscriptionGaps;
        CancellationToken   = cancellationToken;

        MessageQueue = new();
        QueueLocked  = new();
    }
    
    // public KrimsonProcessorContext(KrimsonRecord record, CancellationToken cancellationToken = default) {
    //     Record              = record;
    //     Logger              = Log.Logger;
    //     State               = new InMemoryStateStore();
    //     CancellationToken   = cancellationToken;
    //     MessageQueue        = new();
    //     QueueLocked         = new();
    //     GetSubscriptionGaps = () => Task.FromResult<IReadOnlyCollection<SubscriptionTopicGap>>(null!);
    // }

    public KrimsonRecord       Record              { get; }
    public ILogger             Logger              { get; }
    public IStateStore         State               { get; }
    public CancellationToken   CancellationToken   { get; }
    public GetSubscriptionGaps GetSubscriptionGaps { get; }

    Queue<ProducerRequest> MessageQueue { get; }
    InterlockedBoolean     QueueLocked  { get; }

    public async Task<bool> HasCaughtUp() {
        var gaps = await GetSubscriptionGaps();
        return gaps.All(x => x.CaughtUp);
    }

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