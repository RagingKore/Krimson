// // ReSharper disable CheckNamespace
//
// using Confluent.Kafka;
//
// namespace Krimson.Connectors;
//
// [PublicAPI]
// public class SourceRecord {
//     public static readonly SourceRecord Empty = new() { Timestamp = Timestamp.Default };
//
//     public SourceRecord() {
//         Key       = MessageKey.None;
//         Value     = null!;
//         Headers   = new();
//         Timestamp = new(DateTime.UtcNow);
//     }
//
//     public MessageKey                  Key       { get; set; }
//     public object                      Value     { get; set; }
//     public Timestamp                   Timestamp { get; set; }
//     public Dictionary<string, string?> Headers   { get; set; }
//     public string?                     Topic     { get; set; }
//
//     public bool HasKey      => Key != MessageKey.None;
//     public bool HasTopic    => Topic is not null;
// }