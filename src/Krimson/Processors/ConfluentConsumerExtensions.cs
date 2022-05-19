using System.Runtime.CompilerServices;
using Confluent.Kafka;
using static System.TimeSpan;

namespace Krimson.Processors;

static class ConfluentConsumerExtensions {
    static readonly TimeSpan DefaultRequestTimeout = FromSeconds(10);

    public static IAsyncEnumerable<KrimsonRecord> Records<TValue>(this IConsumer<byte[], TValue> consumer, CancellationToken cancellationToken = default) {
        return Consume().ToAsyncEnumerable();

        IEnumerable<KrimsonRecord> Consume() {
            while (!cancellationToken.IsCancellationRequested) {
                var result = TryConsume(() => consumer.Consume(cancellationToken));

                if (result.Continue)
                    continue; // IsPartitionEOF

                if (result.Break)
                    yield break; // OperationCanceledException

                yield return KrimsonRecord.From(result.Result);
            }
        }

        // because we can't use a try-catch and we simply want to break when/if
        // * Cancellation is requested
        // * OperationCanceledException is thrown
        // * Fatal KafkaException is thrown
        static (ConsumeResult<byte[], TValue> Result, bool Continue, bool Break) TryConsume(Func<ConsumeResult<byte[], TValue>?> consume) {
            try {
                var result = consume();

                if (result is not null && !result.IsPartitionEOF)
                    return (result, false, false);

                return (null, true, false)!;
            }
            catch (KafkaException ex) when (!ex.Error.IsFatal) {
                return (null, true, false)!;
            }
            catch (OperationCanceledException) {
                return (null, false, true)!;
            }
        }
    }

    public static void TrackPosition<TValue>(this IConsumer<byte[], TValue> consumer, TopicPartitionOffset position) =>
        consumer.StoreOffset(new TopicPartitionOffset(position.Topic, position.Partition, position.Offset + 1));

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
        catch (KafkaException ex) when (!ex.Error.IsFatal) {
            // only throw if it is indeed a fatal error
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
        catch (KafkaException ex) when (!ex.Error.IsFatal) {
            // only throw if it is indeed a fatal error
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