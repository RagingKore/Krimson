using Confluent.Kafka;
using static System.DateTimeOffset;

namespace Krimson.Connectors;

[PublicAPI]
public class SourceRecord {
    public static readonly SourceRecord Empty = new() {
        EventTime = MinValue.ToUnixTimeMilliseconds()
    };

    public SourceRecord() {
        Value      = null!;
        Key        = MessageKey.None;
        EventTime  = UtcNow.ToUnixTimeMilliseconds();
        Headers    = new();
        RecordId   = RecordId.None;
        Processing = new();
    }

    public SourceRecord(object value) : this() {
        Value = value;
    }
    
    TaskCompletionSource<RecordId> Processing { get; }
    
    public object                      Value            { get; set; }
    public MessageKey                  Key              { get; set; }
    public long                        EventTime        { get; set; }
    public string?                     DestinationTopic { get; set; }
    public Dictionary<string, string?> Headers          { get; set; }
    public RecordId                    RecordId         { get; private set; }
 
    public bool HasKey              => Key != MessageKey.None;
    public bool HasDestinationTopic => DestinationTopic is not null;
    public bool Processed           => Processing.Task.IsCompleted;
    
    public string? EventType {
        get => Headers.TryGetValue("krimson.connectors.source.record.event-type", out var value) ? value : null;
        set => Headers["krimson.connectors.source.record.event-type"] = value;
    }
    
    public string? Source {
        get => Headers.TryGetValue("krimson.connectors.source.name", out var value) ? value : null;
        internal set => Headers["krimson.connectors.source.name"] = value;
    }

    public void Ack(RecordId recordId)                           => Processing.SetResult(recordId);
    public void Nak(ProduceException<byte[], object?> exception) => Processing.SetException(exception);

    public async Task EnsureProcessed() {
        RecordId = await Processing.Task.ConfigureAwait(false);
    }
}