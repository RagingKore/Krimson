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
        RequestId  = Guid.NewGuid();
        RecordId   = RecordId.None;
        Processing = new();
    }

    public SourceRecord(object value) : this() {
        Value = value;
    }
    
    TaskCompletionSource Processing { get; }
    
    public object                      Value            { get; set; }
    public MessageKey                  Key              { get; set; }
    public long                        EventTime        { get; set; }
    public string?                     DestinationTopic { get; set; }
    public Dictionary<string, string?> Headers          { get; set; }
    public Guid                        RequestId        { get; set; }
    public RecordId                    RecordId         { get; private set; }
 
    public bool HasKey              => Key != MessageKey.None;
    public bool HasDestinationTopic => DestinationTopic is not null;
    
    public bool ProcessingSuccessful => Processing.Task.IsCompletedSuccessfully && RecordId != RecordId.None;
    public bool ProcessingFaulted    => Processing.Task.IsFaulted;
    public bool ProcessingSkipped    => Processing.Task.IsCompletedSuccessfully && RecordId == RecordId.None;
    
    public string? EventType {
        get => Headers.TryGetValue("krimson.connectors.source.record.event-type", out var value) ? value : null;
        set => Headers["krimson.connectors.source.record.event-type"] = value;
    }
    
    public string? Source {
        get => Headers.TryGetValue("krimson.connectors.source.name", out var value) ? value : null;
        internal set => Headers["krimson.connectors.source.name"] = value;
    }

    public void Skip() =>
        Processing.SetResult();

    public void Ack(RecordId recordId) {
        RecordId = recordId;
        Processing.SetResult();
    }

    public void Nak(ProduceException<byte[], object?> exception) => 
        Processing.SetException(exception);

    /// <summary>
    /// A task that finishes once the record was processed regardless of the status. Will throw on error.
    /// </summary>
    public Task EnsureProcessed() => Processing.Task;
}