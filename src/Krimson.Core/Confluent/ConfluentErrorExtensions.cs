using Confluent.Kafka;

namespace Krimson;

public static class ConfluentErrorExtensions {
    public static bool IsUseless(this Error error) => error.Code is ErrorCode.NoError;

    public static bool IsTerminal(this Error error) =>
        error.IsFatal || error.Code
         is ErrorCode.TopicException
         or ErrorCode.Local_KeySerialization
         or ErrorCode.Local_ValueSerialization
         or ErrorCode.Local_KeyDeserialization
         or ErrorCode.Local_ValueDeserialization
         or ErrorCode.OffsetOutOfRange
         or ErrorCode.OffsetMetadataTooLarge
         or ErrorCode.ClusterAuthorizationFailed
         or ErrorCode.TopicAuthorizationFailed
         or ErrorCode.GroupAuthorizationFailed
         or ErrorCode.UnsupportedSaslMechanism
         or ErrorCode.SecurityDisabled
         or ErrorCode.SaslAuthenticationFailed;

    public static bool IsTransient(this Error error) =>
        !error.IsUseless() && !error.IsTerminal() && error.Code
         is ErrorCode.NoError
         or ErrorCode.Local_NoOffset  
         or ErrorCode.Local_QueueFull
         or ErrorCode.OutOfOrderSequenceNumber
         or ErrorCode.TransactionCoordinatorFenced
         or ErrorCode.UnknownProducerId
         or ErrorCode.Local_AllBrokersDown;

    public static KafkaException? AsKafkaException(this Error error) =>
        error.IsUseless() ? null : new KafkaException(error);
}