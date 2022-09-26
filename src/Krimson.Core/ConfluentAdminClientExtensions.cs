using Confluent.Kafka;
using Confluent.Kafka.Admin;
using static System.TimeSpan;
using static System.String;

namespace Krimson;

[PublicAPI]
public static class ConfluentAdminClientExtensions {
    static readonly TimeSpan DefaultRequestTimeout = FromSeconds(10);

    public static async Task<bool> CreateTopic(
        this IAdminClient client,
        string topicName,
        int partitions,
        short replicationFactor,
        Dictionary<string, string>? configs = null
    ) {
        Ensure.NotNullOrWhiteSpace(topicName, nameof(topicName));
        Ensure.Positive(partitions, nameof(partitions));
        Ensure.Positive(replicationFactor, nameof(replicationFactor));

        var entries = await DescribeTopic(client, topicName).ConfigureAwait(false);

        if (entries is not null)
            return true;

        try {
            await client
                .CreateTopicsAsync(
                    new List<TopicSpecification> {
                        new() {
                            Name              = topicName,
                            NumPartitions     = partitions,
                            ReplicationFactor = replicationFactor,
                            Configs           = configs ?? new Dictionary<string, string>()
                        }
                    }
                ).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) {
            if (ex.Error.Code is ErrorCode.Local_Partial && ex.Message.Contains("Topic replication factor must be")) {
                var reportedReplicationFactor = short.Parse(
                    ex.Results.FirstOrDefault()?.Error.Reason.Replace("Topic replication factor must be", Empty).Trim() ?? "1"
                );

                return await CreateTopic(
                    client, topicName, partitions,
                    reportedReplicationFactor, configs
                );
            }

            if (ex.Results.Select(r => r.Error.Code).Any(err => err != ErrorCode.TopicAlreadyExists && err != ErrorCode.NoError))
                throw;
        }

        using var cts = new CancellationTokenSource(DefaultRequestTimeout);

        while (!cts.IsCancellationRequested) {
            entries = await DescribeTopic(client, topicName);

            if (entries is not null)
                return true;

            await Tasks.SafeDelay(FromMilliseconds(250), cts.Token);
        }

        return false;
    }

    public static Task<bool> CreateCompactedTopic(this IAdminClient client, string topicName, int partitions, short replicationFactor) =>
        CreateTopic(
            client, topicName, partitions, replicationFactor, 
            new() { { "cleanup.policy", "compact" } }
        );

    public static Task<bool[]> CreateTopics(
        this IAdminClient client,
        string[] topicNames,
        int partitions,
        short replicationFactor,
        Dictionary<string, string>? configs = null
    ) {
        Ensure.NotNullOrEmpty(topicNames, nameof(topicNames));
        Ensure.Positive(partitions, nameof(partitions));
        Ensure.Positive(replicationFactor, nameof(replicationFactor));

        return topicNames
            .Select(
                name => CreateTopic(
                    client, name, partitions,
                    replicationFactor, configs
                )
            )
            .WhenAll();
    }

    /// <summary>
    /// Gets the configuration for the specified topic.
    /// A null result will be returned if the topic does not exist.
    /// </summary>
    public static async ValueTask<Dictionary<string, ConfigEntryResult>?> DescribeTopic(this IAdminClient client, string topic) {
        Ensure.NotNullOrWhiteSpace(topic, nameof(topic));

        try {
            var result = await client.DescribeConfigsAsync(
                new[] {
                    new ConfigResource {
                        Name = topic,
                        Type = ResourceType.Topic
                    }
                }
            );

            return result[0].Entries;
        }
        catch (DescribeConfigsException ex) {
            if (ex.Results[0].Error.Code == ErrorCode.UnknownTopicOrPart)
                return null;

            throw;
        }
    }

    public static async ValueTask<Dictionary<string, ConfigEntryResult>?> DescribeConfigurations(
        this IAdminClient client, string resource, ResourceType resourceType
    ) {
        Ensure.NotNullOrWhiteSpace(resource, nameof(resource));

        var result = await client
            .DescribeConfigsAsync(
                new[] {
                    new ConfigResource {
                        Name = resource,
                        Type = resourceType
                    }
                }
            )
            .ConfigureAwait(false);

        return result[0].Entries;
    }

    public static async Task DeleteTopics(this IAdminClient client, List<string> topics) {
        Ensure.NotNull(topics, nameof(topics));

        try {
            await client.DeleteTopicsAsync(topics).ConfigureAwait(false);
        }
        catch (DeleteTopicsException ex) {
            if (ex.Results.Select(r => r.Error.Code).Any(err => err != ErrorCode.UnknownTopicOrPart && err != ErrorCode.NoError))
                throw new Exception("Unable to delete topics", ex);
        }
    }

    public static Task DeleteTopic(this IAdminClient client, string topic) =>
        DeleteTopics(client, new() { topic });

    public static async Task DeleteTestTopics(this IAdminClient client, Predicate<string> predicate) {
        var topicsToDelete = client
            .GetMetadata(DefaultRequestTimeout * 6)
            .Topics.Where(x => predicate(x.Topic))
            .Select(x => x.Topic)
            .ToList();

        if (topicsToDelete.Any())
            await client
                .DeleteTopics(topicsToDelete)
                .ConfigureAwait(false);
    }

    public static IEnumerable<PartitionMetadata> GetTopicPartitions(this IAdminClient client, string topic) =>
        client.GetMetadata(topic, DefaultRequestTimeout).Topics.SingleOrDefault()?.Partitions ?? Enumerable.Empty<PartitionMetadata>();
}