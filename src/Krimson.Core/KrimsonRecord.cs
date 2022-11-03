using Confluent.Kafka;
using static System.String;

namespace Krimson;

[PublicAPI]
public record KrimsonRecord {
    public RecordId                     Id          { get; internal init; } = null!;
    public MessageKey                   Key         { get; internal init; } = MessageKey.None;
    public object                       Value       { get; internal init; } = null!;
    public Timestamp                    Timestamp   { get; internal init; }
    public IDictionary<string, string?> Headers     { get; internal init; } = null!;
    public Type                         MessageType { get; internal init; } = null!;


    public TopicPartitionOffset Position  => Id.Position;

    public string    Topic     => Id.Position.Topic;
    public Partition Partition => Id.Position.Partition;
    public Offset    Offset    => Id.Position.Offset;

    public string RequestId    => Headers.TryGetValue(HeaderKeys.RequestId, out var value) ? value! : Empty;
    public string ProducerName => Headers.TryGetValue(HeaderKeys.ProducerName, out var value) ? value! : Empty;
    public int    SchemaId     => Headers.TryGetValue(HeaderKeys.SchemaId, out var value) ? int.Parse(value ?? Empty) : -1;

    public bool HasKey          => Key != MessageKey.None;
    public bool HasRequestId    => RequestId != Empty;
    public bool HasProducerName => ProducerName != string.Empty;
    public bool HasSchemaId     => SchemaId > -1;
    
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
    
    public static KrimsonRecord From(RecordId id, MessageKey key, object value, Timestamp timestamp, Headers headers) {
        return new() {
            Id          = id,
            Key         = key,
            Value       = value,
            Timestamp   = timestamp,
            Headers     = headers.Decode(),
            MessageType = value.GetType()
        };
    }
    
    public static KrimsonRecord From(RecordId id, MessageKey key, object value, DateTimeOffset timestamp, IDictionary<string, string?> headers) {
        return new() {
            Id          = id,
            Key         = key,
            Value       = value,
            Timestamp   = new(timestamp),
            Headers     = headers,
            MessageType = value.GetType()
        };
    }
}