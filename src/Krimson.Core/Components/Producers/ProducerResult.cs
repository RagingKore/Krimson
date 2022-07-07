using Confluent.Kafka;

namespace Krimson.Producers;

[PublicAPI]
public record ProducerResult {
    static readonly Error NoError = new Error(ErrorCode.NoError);

    public Guid                               RequestId         { get; private init; }
    public DateTimeOffset                     Timestamp         { get; private init; }
    public RecordId                           RecordId          { get; private init; } = RecordId.None;
    public PersistenceStatus                  PersistenceStatus { get; private init; }
    public Error                              Error             { get; private init; } = NoError;
    public ProduceException<byte[], object?>? Exception         { get; private init; }

    public bool Success => Error.IsUseless();
    
    public static ProducerResult From(Guid requestId, DeliveryReport<byte[], object?> report) {
        return new() {
            RequestId         = requestId,
            Timestamp         = DateTimeOffset.FromUnixTimeMilliseconds(report.Timestamp.UnixTimestampMs),
            RecordId          = report.Error.IsError ? RecordId.None : RecordId.From(report.TopicPartitionOffset),
            PersistenceStatus = report.Status,
            Error             = report.Error,
            Exception         = report.Error.IsError ? new ProduceException<byte[], object?>(report.Error, report) : null
        };
    }
}