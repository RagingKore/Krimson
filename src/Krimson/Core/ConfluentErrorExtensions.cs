using Confluent.Kafka;

namespace Krimson;

static class ConfluentErrorExtensions {
    public static bool IsUseless(this Error error)   => error.Code is ErrorCode.NoError;
    public static bool IsTerminal(this Error error)  => error.IsFatal;
    public static bool IsTransient(this Error error) => !error.IsUseless() && !error.IsTerminal();
    
    public static KafkaException? AsKafkaException(this Error error) =>
        error.IsUseless() ? null : new KafkaException(error);
}