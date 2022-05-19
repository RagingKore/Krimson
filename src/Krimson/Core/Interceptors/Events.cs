using Confluent.Kafka;

namespace Krimson.Interceptors;

public abstract record InterceptorEvent {
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

public record ConfluentConsumerError(string ConsumerName, KafkaException Exception) : InterceptorEvent;

public record ConfluentProducerError(string ProducerName, KafkaException Exception) : InterceptorEvent;

