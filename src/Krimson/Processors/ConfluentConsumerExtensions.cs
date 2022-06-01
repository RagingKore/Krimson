using System.Runtime.CompilerServices;
using Confluent.Kafka;
using static System.TimeSpan;

namespace Krimson.Processors;


public delegate void OnPartitionEndReached(TopicPartitionOffset position);

static class ConfluentConsumerExtensions {
    static readonly TimeSpan DefaultRequestTimeout = FromSeconds(10);

    public static IAsyncEnumerable<KrimsonRecord> Records<TValue>(this IConsumer<byte[], TValue> consumer, OnPartitionEndReached partitionEndReached, CancellationToken cancellationToken = default) {
        return Consume().ToAsyncEnumerable();

        IEnumerable<KrimsonRecord> Consume() {
            while (!cancellationToken.IsCancellationRequested) {
                var result = TryConsume(() => consumer.Consume(cancellationToken));

                if (result.HasCaughtUp) {
                    try {
                        partitionEndReached(result.ConsumeResult.TopicPartitionOffset);
                    }
                    catch (Exception) {
                        // TODO SS: consider logging this error for debug
                    }

                    continue;
                }
                
                if (result.Continue)
                    continue; // transient error

                if (result.Break)
                    yield break; // OperationCanceledException

                yield return KrimsonRecord.From(result.ConsumeResult);
            }
        }

        // TODO SS: add oneof or something for a more interesting and clean functional solution
        // because we can't use a try-catch and we simply want to break when/if
        // * Cancellation is requested
        // * OperationCanceledException is thrown
        // * Fatal KafkaException is thrown
        static (ConsumeResult<byte[], TValue> ConsumeResult, bool Continue, bool Break, bool HasCaughtUp) TryConsume(Func<ConsumeResult<byte[], TValue>?> consume) {
            try {
                var result = consume();

                if (result is not null)
                    return result.IsPartitionEOF 
                        ? (result, false, false, true) 
                        : (result, false, false, false);
                
                return (null, true, false, false)!;
            }
            catch (KafkaException kex) when (kex.IsTransient()) {
                return (null, true, false, false)!;
            }
            catch (OperationCanceledException) {
                return (null, false, true, false)!;
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

[PublicAPI]
public record SubscriptionTopicGap(string Topic, IReadOnlyCollection<SubscriptionPartitionGap> PartitionGaps, DateTimeOffset Timestamp) {
    public bool CaughtUp => PartitionGaps.All(x => x.CaughtUp);
}

[PublicAPI]
public record SubscriptionPartitionGap(Partition Partition, Offset LastPosition, Offset CommitPosition) {
    public long Gap { get; } = LastPosition.Value - CommitPosition.Value;
    
    public bool CaughtUp => Gap == 0;
}