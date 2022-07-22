using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.Processors;
using Krimson.Processors.Interceptors;
using Krimson.Readers.Configuration;
using Serilog;

namespace Krimson.Readers;

[PublicAPI]
public sealed class KrimsonReader {
    public static KrimsonReaderBuilder Builder => new();

    public KrimsonReader(KrimsonReaderOptions options) {
        Options  = options;
        ClientId = options.ConsumerConfiguration.ClientId;
        GroupId  = options.ConsumerConfiguration.GroupId;
        Logger   = Log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, ClientId);

        Intercept = options.Interceptors
            .Prepend(new ConfluentProcessorLogger())
            .Intercept;
    }

    KrimsonReaderOptions  Options   { get; }
    ILogger               Logger    { get; }
    ISchemaRegistryClient Registry  { get; }
    Intercept             Intercept { get; }

    public string ClientId { get; }
    public string GroupId  { get; }

    public async IAsyncEnumerable<KrimsonRecord> Records(TopicPartitionOffset startPosition, [EnumeratorCancellation] CancellationToken cancellationToken) {
        using var consumer = new ConsumerBuilder<byte[], object?>(Options.ConsumerConfiguration)
            .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
            .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
            .SetValueDeserializer(Options.DeserializerFactory())
            // .SetPartitionsAssignedHandler((_, partitions) => Intercept(new PartitionsAssigned(ClientId, partitions)))
            // .SetOffsetsCommittedHandler((_, committed) => Intercept(new PositionsCommitted(ClientId, committed.Offsets, committed.Error)))
            // .SetPartitionsRevokedHandler(
            //     (_, positions) => {
            //         Intercept(new PartitionsRevoked(ClientId, positions));
            //         Flush(positions);
            //     }
            // )
            // .SetPartitionsLostHandler(
            //     (_, positions) => {
            //         Intercept(new PartitionsLost(ClientId, positions));
            //         Flush(positions);
            //     }
            // )
            .Build();

        consumer.Assign(startPosition);

        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // ReSharper disable once AccessToDisposedClosure
        await foreach (var record in consumer.Records(partitionEndReached: _ => cancellator.Cancel(), cancellator.Token).ConfigureAwait(false))
            yield return record;

        await consumer.Stop().ConfigureAwait(false);
    }

    public async Task Process(TopicPartitionOffset startPosition, Func<KrimsonRecord, Task> handler, CancellationToken cancellationToken) {
        await foreach (var record in Records(startPosition, cancellationToken).ConfigureAwait(false)) {
            await handler(record).ConfigureAwait(false);
        }
    }

    public IAsyncEnumerable<KrimsonRecord> Records(string topic, CancellationToken cancellationToken) =>
        Records(new TopicPartitionOffset(topic, Partition.Any, Offset.Beginning), cancellationToken);

    public Task Process(string topic, Func<KrimsonRecord, Task> handler, CancellationToken cancellationToken) =>
        Process(new TopicPartitionOffset(topic, Partition.Any, Offset.Beginning), handler, cancellationToken);
    
    public async Task<IReadOnlyCollection<SubscriptionTopicGap>> GetSubscriptionGap(string topic, CancellationToken cancellationToken = default) {
        using var consumer = new ConsumerBuilder<byte[], object?>(Options.ConsumerConfiguration)
            .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
            .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
            .SetValueDeserializer(Options.DeserializerFactory())
            .Build();


        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var gap = await consumer.GetSubscriptionGap(cancellator.Token);

        await consumer.Stop().ConfigureAwait(false);

        return gap;
    }
    
    public async Task<List<TopicPartitionOffset>> GetTopicLatestPositions(string topic, CancellationToken cancellationToken = default) {
        using var consumer = new ConsumerBuilder<byte[], object?>(Options.ConsumerConfiguration)
            .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
            .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
            .SetValueDeserializer(Options.DeserializerFactory())
            .Build();


        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var positions = await consumer.GetTopicLatestPositions(topic, cancellator.Token).ToListAsync(cancellator.Token);

        await consumer.Stop().ConfigureAwait(false);

        return positions;
    }

     async ValueTask<KrimsonRecord?> GetLastRecord(string topic, CancellationToken cancellationToken = default) {
        var lastStoredPosition = new TopicPartitionOffset(new TopicPartition(topic, Partition.Any), Offset.Stored);

        var record = await Records(lastStoredPosition, cancellationToken)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return record;
    }

    public ValueTask DisposeAsync() {
        return ValueTask.CompletedTask;
    }
}