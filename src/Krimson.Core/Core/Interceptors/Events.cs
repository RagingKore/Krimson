using Confluent.Kafka;

namespace Krimson.Interceptors;

public abstract record InterceptorEvent {
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

public record ConfluentConsumerError(string ConsumerName, string ClientInstanceId, Error Error) : InterceptorEvent;

public record ConfluentConsumerLog(string ConsumerName, string ClientInstanceId, LogMessage LogMessage) : InterceptorEvent;

public record ConfluentProducerError(string ProducerName, string ClientInstanceId, Error Error) : InterceptorEvent;

public record ConfluentProducerLog(string ProducerName, string ClientInstanceId, LogMessage LogMessage) : InterceptorEvent;