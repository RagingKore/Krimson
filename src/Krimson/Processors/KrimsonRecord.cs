using Confluent.Kafka;

namespace Krimson.Processors;

[PublicAPI]
public record KrimsonRecord {
    public RecordId                             Id          { get; internal init; } = null!;
    public MessageKey                           Key         { get; internal init; } = MessageKey.None;
    public object                               Value       { get; internal init; } = null!;
    public Timestamp                            Timestamp   { get; internal init; }
    public IReadOnlyDictionary<string, string?> Headers     { get; internal init; } = null!;
    public Type                                 MessageType { get; internal init; } = null!;


    public TopicPartitionOffset Position  => Id.Position;

    public string    Topic     => Id.Position.Topic;
    public Partition Partition => Id.Position.Partition;
    public Offset    Offset    => Id.Position.Offset;

    public string RequestId    => Headers.TryGetValue(HeaderKeys.RequestId, out var value) ? value! : "";
    public string ProducerName => Headers.TryGetValue(HeaderKeys.ProducerName, out var value) ? value! : "";
    
    public bool HasKey          => Key != MessageKey.None;
    public bool HasRequestId    => RequestId != "";
    public bool HasProducerName => ProducerName != "";
    
    public override string ToString() => $"{Topic}:{Partition.Value}@{Offset}";

    public static KrimsonRecord From<TValue>(ConsumeResult<byte[], TValue> result) {
        return new() {
            Id          = RecordId.From(result.TopicPartitionOffset),
            Key         = MessageKey.From(result.Message.Key),
            Value       = result.Message.Value!,
            Timestamp   = result.Message.Timestamp,
            Headers     = result.Message.Headers.Decode(),
            MessageType = result.Message.Value!.GetType()
        };
    }
}