using Confluent.Kafka;

namespace Krimson;

[PublicAPI]
public static class ConfluentKafkaExceptionExtensions {
    public static bool IsUseless(this KafkaException exception)   => exception.Error.IsUseless();
    public static bool IsTerminal(this KafkaException exception)  => exception.Error.IsTerminal();
    public static bool IsRetryable(this KafkaException exception) => exception is KafkaRetriableException;
    public static bool IsTransient(this KafkaException exception) => exception.Error.IsTransient() || exception.IsRetryable();
}