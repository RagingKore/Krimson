using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Producers;

namespace Krimson.Processors.Interceptors;

public abstract record ProcessorEvent(string Processor) : InterceptorEvent;

public record ProcessorStarted(string Processor, string SubscriptionName, string[] Topics) : ProcessorEvent(Processor);

public record ProcessorStopping(string Processor, string SubscriptionName, string[] Topics) : ProcessorEvent(Processor);

public record ProcessorStopped(string Processor, string SubscriptionName, string[] Topics, Exception? Error = null) : ProcessorEvent(Processor);

/// <summary>
/// Tracked positions/offsets committed to the broker or an error while doing so.
/// </summary>
public record PositionsCommitted(string Processor, IList<TopicPartitionOffsetError> Positions, Exception? Error = null) : ProcessorEvent(Processor);

/// <summary>
/// Topic partitions assigned for processing.
/// </summary>
public record PartitionsAssigned(string Processor, IReadOnlyCollection<TopicPartition> Partitions) : ProcessorEvent(Processor);

/// <summary>
/// Topic partitions to stop processing.
/// </summary>
public record PartitionsRevoked(string Processor, IReadOnlyCollection<TopicPartitionOffset> Positions) : ProcessorEvent(Processor);

/// <summary>
/// Topic partitions to stop processing but in a spectacular and unknown way...
/// </summary>
public record PartitionsLost(string Processor, IReadOnlyCollection<TopicPartitionOffset> Positions) : ProcessorEvent(Processor);

/// <summary>
/// The processor has caught up and processed all available records int this topic partition
/// </summary>
public record PartitionEndReached(string Processor, TopicPartitionOffset Position) : ProcessorEvent(Processor);

/// <summary>
/// Record processing skipped because there are no user handlers registered.
/// </summary>
public record InputSkipped(string Processor, KrimsonRecord Record) : ProcessorEvent(Processor);

/// <summary>
/// Record received, deserialized and waiting to be processed.
/// </summary>
public record InputReady(string Processor, KrimsonRecord Record) : ProcessorEvent(Processor);

/// <summary>
/// Record handled by user processing logic.
/// </summary>
public record InputConsumed(string Processor, KrimsonRecord Record, IReadOnlyCollection<ProducerRequest> Output) : ProcessorEvent(Processor);

/// <summary>
/// Record output messages were sent and acknowledged by the broker and topic position was tracked  commit.
/// </summary>
public record InputProcessed(string Processor, KrimsonRecord Record, IReadOnlyCollection<ProducerRequest> Output) : ProcessorEvent(Processor);

/// <summary>
/// Record consumption and processing failed.
/// </summary>
public record InputError(string Processor, KrimsonRecord Record, IReadOnlyCollection<ProducerRequest> Output, Exception Error) : ProcessorEvent(Processor) {
    public InputError(string processor, KrimsonRecord record, Exception error) : this(
        processor, record, new List<ProducerRequest>(), error
    ) { }
}

// /// <summary>
// /// Record positions tracked for deferred acknowledgement.
// /// </summary>
// public record InputPositionTracked(string Processor, KafkaRecord Record) : ProcessorEvent(Processor);
//
//
// /// <summary>
// /// Before attempting to send the output message
// /// </summary>
// public record OutputReady(string ProcessorName, string ProducerName, ProducerMessage Message, KafkaRecord Input) : InterceptorEvent;

/// <summary>
/// After attempting to send the output message
/// </summary>
public record OutputProcessed(string ProcessorName, string ProducerName, ProducerResult Result, KrimsonRecord Input) : InterceptorEvent;


