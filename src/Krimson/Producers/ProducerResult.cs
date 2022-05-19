using Confluent.Kafka;

namespace Krimson.Producers;

[PublicAPI]
public record ProducerResult {
    public Guid                               RequestId { get; private init; }
    public DateTimeOffset                     Timestamp { get; private init; }
    public RecordId                           RecordId  { get; private init; } = RecordId.None;
    public ProduceException<byte[], object?>? Exception { get; private init; }

    public bool DeliveryFailed  => Exception is not null;
    public bool RecordPersisted => RecordId != RecordId.None;

    public static ProducerResult From(Guid requestId, DeliveryReport<byte[], object?> report) {
        return report.Error.IsError switch {
            true => new() {
                RequestId = requestId,
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(report.Timestamp.UnixTimestampMs),
                Exception = new(
                    new(report.Error.Code, $"{(report.Error.IsFatal ? "Fatal Error " : "")}{report.Error.Reason}", report.Error.IsFatal), report
                )
            },
            false => new() {
                RequestId = requestId,
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(report.Timestamp.UnixTimestampMs),
                RecordId  = RecordId.From(report.TopicPartitionOffset)
            }
        };
    }
}