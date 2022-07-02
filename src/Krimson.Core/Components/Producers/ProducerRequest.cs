namespace Krimson.Producers;

[PublicAPI]
public record ProducerRequest {
    public static ProducerRequestBuilder Builder => new();

    public static readonly ProducerRequest Empty = new ProducerRequest();
    
    ProducerRequest() { }

    public ProducerRequest(object message) {
        Message   = message;
        Type      = message.GetType();
        Headers   = new Dictionary<string, string?>();
        RequestId = Guid.NewGuid();
    }

    public object                      Message   { get; internal init; } = null!;
    public MessageKey                  Key       { get; internal init; } = MessageKey.None;
    public Type                        Type      { get; internal init; } = null!;
    public Dictionary<string, string?> Headers   { get; internal init; } = null!;
    public Guid                        RequestId { get; internal init; } = Guid.Empty;
    public string?                     Topic     { get; internal init; }

    public bool HasKey   => Key != MessageKey.None;
    public bool HasTopic => Topic is not null;
}