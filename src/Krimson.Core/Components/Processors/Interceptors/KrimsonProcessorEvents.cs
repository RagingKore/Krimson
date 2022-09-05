using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Producers;

namespace Krimson.Processors.Interceptors;

public abstract record ProcessorEvent(IKrimsonProcessorInfo Processor) : InterceptorEvent;

public record ProcessorConfigured(IKrimsonProcessorInfo Processor) : ProcessorEvent(Processor);

public record ProcessorActivated(IKrimsonProcessorInfo Processor) : ProcessorEvent(Processor);

public record ProcessorTerminating(IKrimsonProcessorInfo Processor) : ProcessorEvent(Processor);

public record ProcessorTerminated(
    IKrimsonProcessorInfo Processor, IReadOnlyCollection<SubscriptionTopicGap> Gaps, Exception? Exception = null
) : ProcessorEvent(Processor);

public record ProcessorTerminatedUserHandlingError(IKrimsonProcessorInfo Processor, Exception? Exception = null) : ProcessorEvent(Processor);

/// <summary>
/// Tracked positions/offsets committed to the broker or an error while doing so.
/// </summary>
public record PositionsCommitted(IKrimsonProcessorInfo Processor, IList<TopicPartitionOffsetError> Positions, Error Error) : ProcessorEvent(Processor);

/// <summary>
/// Topic partitions assigned for processing.
/// </summary>
public record PartitionsAssigned(IKrimsonProcessorInfo Processor, IReadOnlyCollection<TopicPartition> Partitions) : ProcessorEvent(Processor);

/// <summary>
/// Topic partitions to stop processing.
/// </summary>
public record PartitionsRevoked(IKrimsonProcessorInfo Processor, IReadOnlyCollection<TopicPartitionOffset> Positions) : ProcessorEvent(Processor);

/// <summary>
/// Topic partitions to stop processing but in a spectacular and unknown way...
/// </summary>
public record PartitionsLost(IKrimsonProcessorInfo Processor, IReadOnlyCollection<TopicPartitionOffset> Positions) : ProcessorEvent(Processor);

/// <summary>
/// The processor has caught up and processed all available records int this topic partition
/// </summary>
public record PartitionEndReached(IKrimsonProcessorInfo Processor, TopicPartitionOffset Position) : ProcessorEvent(Processor);

/// <summary>
/// Record processing skipped because there are no user handlers registered.
/// </summary>
public record InputSkipped(IKrimsonProcessorInfo Processor, KrimsonRecord Record) : ProcessorEvent(Processor);

/// <summary>
/// Record received, deserialized and waiting to be processed.
/// </summary>
public record InputReady(IKrimsonProcessorInfo Processor, KrimsonRecord Record) : ProcessorEvent(Processor);

/// <summary>
/// Record handled by user processing logic.
/// </summary>
public record InputConsumed(IKrimsonProcessorInfo Processor, KrimsonRecord Record, IReadOnlyCollection<ProducerRequest> Output) : ProcessorEvent(Processor);

/// <summary>
/// Record output messages were sent and acknowledged by the broker and topic position was tracked  commit.
/// </summary>
public record InputProcessed(IKrimsonProcessorInfo Processor, KrimsonRecord Record, IReadOnlyCollection<ProducerRequest> Output) : ProcessorEvent(Processor);

/// <summary>
/// Record consumption and processing failed.
/// </summary>
public record InputError(
    IKrimsonProcessorInfo Processor, KrimsonRecord Record, IReadOnlyCollection<ProducerRequest> Output, Exception Exception
) : ProcessorEvent(Processor) {
    public InputError(IKrimsonProcessorInfo processor, KrimsonRecord record, Exception exception) : this(
        processor, record, new List<ProducerRequest>(),
        exception
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
// public record OutputReady(string ClientId, string ProducerName, ProducerMessage Message, KafkaRecord Input) : InterceptorEvent;

/// <summary>
/// After attempting to send the output message
/// </summary>
public record OutputProcessed(IKrimsonProcessorInfo Processor, string ProducerId, ProducerResult Result, KrimsonRecord Input, ProducerRequest Request) : InterceptorEvent;