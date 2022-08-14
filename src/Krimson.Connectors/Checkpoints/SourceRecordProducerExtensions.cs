using Krimson.Producers;

namespace Krimson.Connectors.Checkpoints;

public static class SourceRecordProducerExtensions {
    public static async ValueTask<SourceRecord> PushSourceRecord(this KrimsonProducer producer, SourceRecord record, bool synchronousDelivery = true) {
        var request = ProducerRequest.Builder
            .Key(record.Key)
            .Message(record.Value)
            .Timestamp(record.EventTime)
            .Headers(record.Headers)
            .Topic(record.DestinationTopic)
            .Create();

        if (synchronousDelivery) {
            var result = await producer
                .Produce(request, throwOnError: false)
                .ConfigureAwait(false);

            HandleResult(record, result);
        }
        else
            producer.Produce(request, result => HandleResult(record, result));

        return record;
        
        static void HandleResult(SourceRecord record, ProducerResult result) {
            if (result.Success)
                record.Ack(result.RecordId);
            else
                record.Nak(result.Exception!);
        }
    }
}