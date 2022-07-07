using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Krimson.Processors;
using OneOf.Types;
using static System.TimeSpan;

namespace Krimson;

public static class ConfluentConsumerExtensions {
    static readonly TimeSpan DefaultRequestTimeout = FromSeconds(10);

    public static IAsyncEnumerable<KrimsonRecord> Records<TValue>(this IConsumer<byte[], TValue> consumer, OnPartitionEndReached partitionEndReached, CancellationToken cancellationToken = default) {
        return Consume().ToAsyncEnumerable();
        
        IEnumerable<KrimsonRecord> Consume() {
            while (!cancellationToken.IsCancellationRequested) {
                var result = TryConsume(() => consumer.Consume(cancellationToken));

                if (result.Value is ConsumeResult<byte[], TValue> consumeResult) {
                    yield return KrimsonRecord.From(consumeResult);
                    continue;
                }
                
                if (result.Value is OperationCanceledException)
                    yield break;

                if (result.Value is ConsumeException cex) {
                    if (cex.IsTerminal())
                        throw cex;
                        
                    continue;
                }

                if (result.Value is TopicPartitionOffset position)
                    partitionEndReached(position);
            }
        }
        
        static ConsumeResult<TValue> TryConsume(Func<ConsumeResult<byte[], TValue>?> consume) {
            try {
                var result = consume();

                return result is not null 
                    ? result.IsPartitionEOF ? result.TopicPartitionOffset : result 
                    : new None();
            }
            catch (Exception ex) {
                return ex;
            }
        }
    }

    public static IAsyncEnumerable<KrimsonRecord> Records<TValue>(this IConsumer<byte[], TValue> consumer, CancellationToken cancellationToken = default) {
        return Records(consumer, _ => { }, cancellationToken);
    }
    
    public static void TrackPosition<TValue>(this IConsumer<byte[], TValue> consumer, TopicPartitionOffset position) {
        try {
            consumer.StoreOffset(new TopicPartitionOffset(position.Topic, position.Partition, position.Offset + 1));
        }
        catch (KafkaException kex) when (kex.IsTransient()) {
            // only throw on terminal error
        }
    }

    public static void TrackPosition<TValue>(this IConsumer<byte[], TValue> consumer, KrimsonRecord record) => TrackPosition(consumer, record.Position);

    public static async IAsyncEnumerable<TopicPartitionOffset> GetTopicLatestPositions<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer, string topic, [EnumeratorCancellation] CancellationToken cancellationToken = default
    ) {
        foreach (var partition in await GetPartitions())
            yield return await GetLatestPartitionOffset(partition);

        Task<TopicPartition[]> GetPartitions() =>
            Task.Run(
                () => {
                    using var client = new DependentAdminClientBuilder(consumer.Handle).Build();

                    return client.GetMetadata(topic, DefaultRequestTimeout).Topics.FirstOrDefault()
                        ?.Partitions.Select(x => new TopicPartition(topic, x.PartitionId)).ToArray() ?? Array.Empty<TopicPartition>();
                }, cancellationToken
            );

        Task<TopicPartitionOffset> GetLatestPartitionOffset(TopicPartition partition) =>
            Task.Run(
                () => {
                    var offsets = consumer.QueryWatermarkOffsets(partition, DefaultRequestTimeout);
                    return new TopicPartitionOffset(partition, offsets.High.Value);
                }, cancellationToken
            );
    }

    public static async Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGap<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer, CancellationToken cancellationToken = default
    ) {
        if (consumer.Subscription.None())
            return Array.Empty<SubscriptionTopicGap>();

        // because it's a blocking call...
        var committedOffsets = await Task.Run(
            () => consumer
                .Committed(DefaultRequestTimeout)
                .ToDictionary(x => x.TopicPartition, x => x.Offset.Value),
            cancellationToken
        );

        return consumer.Assignment
            .GroupBy(x => x.Topic)
            .Select(partitions => new SubscriptionTopicGap(partitions.Key, GetGaps(partitions), DateTimeOffset.UtcNow))
            .ToArray();

        IReadOnlyCollection<SubscriptionPartitionGap> GetGaps(IEnumerable<TopicPartition> partitionsByTopic) {
            return partitionsByTopic.Select(
                topicPartition => {
                    var lastOffset = consumer.GetWatermarkOffsets(topicPartition).High;

                    if (lastOffset == 0)
                        lastOffset = Offset.Unset;

                    if (!committedOffsets.TryGetValue(topicPartition, out var committedOffset)) {
                        var lastCommittedOffset = consumer.Position(topicPartition);
                        committedOffset = lastCommittedOffset == Offset.Unset ? 0 : lastCommittedOffset;
                    }

                    return new SubscriptionPartitionGap(topicPartition.Partition, lastOffset, committedOffset);
                }
            ).ToArray();
        }
    }

    public static List<TopicPartitionOffset> CommitAll<TKey, TValue>(this IConsumer<TKey, TValue> consumer) {
        try {
            return consumer.Commit();
        }
        //catch (KafkaException kex) when (kex.IsTransient() || kex.Error.Code == ErrorCode.Local_NoOffset) {
        catch (KafkaException kex) when(kex.IsTransient()) {
            // only throw on terminal error
            return new();
        }
    }

    /// <summary>
    /// Commits offsets (if auto commit is enabled),
    /// alerts the group coordinator
    /// that the consumer is exiting the group then
    /// releases all resources used by this consumer.
    /// </summary>
    public static async Task<IReadOnlyCollection<SubscriptionTopicGap>> Stop<TKey, TValue>(this IConsumer<TKey, TValue> consumer) {
        consumer.CommitAll();

        var gap = await consumer
            .GetSubscriptionGap()
            .ConfigureAwait(false);
        
        try {
            consumer.Close();
        }
        catch (KafkaException kex) when (kex.IsTransient()) {
            // only throw on terminal error
        }

        return gap;
    }
}

public delegate void OnPartitionEndReached(TopicPartitionOffset position);

class ConsumeResult<TValue> : OneOfBase<ConsumeResult<byte[], TValue>, None, Exception, TopicPartitionOffset> {
    ConsumeResult(OneOf<ConsumeResult<byte[], TValue>, None, Exception, TopicPartitionOffset> input) : base(input) { }
    
    public static implicit operator ConsumeResult<TValue>(ConsumeResult<byte[], TValue> _) => new ConsumeResult<TValue>(_);
    public static implicit operator ConsumeResult<TValue>(None _)                          => new ConsumeResult<TValue>(_);
    public static implicit operator ConsumeResult<TValue>(Exception _)                     => new ConsumeResult<TValue>(_);
    public static implicit operator ConsumeResult<TValue>(TopicPartitionOffset _)          => new ConsumeResult<TValue>(_);
    
    public static explicit operator ConsumeResult<byte[], TValue>(ConsumeResult<TValue> _) => _.AsT0;
    public static explicit operator None(ConsumeResult<TValue> _)                          => _.AsT1;
    public static explicit operator Exception(ConsumeResult<TValue> _)                     => _.AsT2;
    public static explicit operator TopicPartitionOffset(ConsumeResult<TValue> _)          => _.AsT3;
}

[PublicAPI]
public record SubscriptionTopicGap(string Topic, IReadOnlyCollection<SubscriptionPartitionGap> PartitionGaps, DateTimeOffset Timestamp) {
    public bool CaughtUp => PartitionGaps.All(x => x.CaughtUp);
}

[PublicAPI]
public record SubscriptionPartitionGap(Partition Partition, Offset LastPosition, Offset CommitPosition) {
    public long Gap { get; } = LastPosition.Value - CommitPosition.Value;
    
    public bool CaughtUp => Gap == 0;
}