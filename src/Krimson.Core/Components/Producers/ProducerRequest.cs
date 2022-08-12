using Confluent.Kafka;

namespace Krimson.Producers;

[PublicAPI]
public record ProducerRequest {
    public static ProducerRequestBuilder Builder => new();

    public static readonly ProducerRequest Empty = new ProducerRequest();
    
    ProducerRequest() { }

    public ProducerRequest(object message) {
        Message   = message;
        Type      = message.GetType();
        Headers   = new();
        RequestId = Guid.NewGuid();
    }

    public object                      Message   { get; internal init; } = null!;
    public MessageKey                  Key       { get; internal init; } = MessageKey.None;
    public Type                        Type      { get; internal init; } = null!;
    public Dictionary<string, string?> Headers   { get; internal init; } = null!;
    public Guid                        RequestId { get; internal init; } = Guid.Empty;
    public Timestamp                   Timestamp { get; internal init; } = new Timestamp(DateTime.UtcNow);
    public string?                     Topic     { get; internal init; }

    public bool HasKey   => Key != MessageKey.None;
    public bool HasTopic => Topic is not null;
}

public static class Voodoo {
    public static ProducerRequestBuilder ProduceRequest(this object source) => 
        ProducerRequest.Builder.Message(source);

    public static ProducerRequest ToProduceRequest(this object source) => 
        ProducerRequest.Builder.Message(source).Create();
    
    public static ProducerRequest ToProduceRequest(this object source, MessageKey key) => 
        ProducerRequest.Builder.Message(source).Key(key).Create();
    
    public static ProducerRequest ToProduceRequest(this object source, MessageKey key, Guid requestId) => 
        ProducerRequest.Builder.Message(source).Key(key).RequestId(requestId).Create();
}