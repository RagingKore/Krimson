using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.Processors;
using Krimson.Processors.Interceptors;
using Krimson.Readers.Configuration;
using Microsoft.Extensions.Logging;

namespace Krimson.Readers;

[PublicAPI]
public sealed class KrimsonReader {
    public static KrimsonReaderBuilder Builder => new();

    public KrimsonReader(KrimsonReaderOptions options) {
        Options  = options;
        ClientId = options.ConsumerConfiguration.ClientId;
        GroupId  = options.ConsumerConfiguration.GroupId;
        Logger   = options.LoggerFactory.CreateLogger(ClientId);
        Registry = options.RegistryFactory();

        Intercept = options.Interceptors
            .Prepend(new ConfluentProcessorLogger())
            .WithLoggerFactory(options.LoggerFactory)
            .Intercept;
    }

    KrimsonReaderOptions  Options   { get; }
    ILogger               Logger    { get; }
    ISchemaRegistryClient Registry  { get; }
    Intercept             Intercept { get; }

    public string ClientId { get; }
    public string GroupId  { get; }

    public async IAsyncEnumerable<KrimsonRecord> Records(TopicPartitionOffset position, [EnumeratorCancellation] CancellationToken cancellationToken) {
        var config = new ConsumerConfig(new Dictionary<string, string>(Options.ConsumerConfiguration)) {
            EnableAutoCommit      = false,
            EnableAutoOffsetStore = false
        };

        using var consumer = new ConsumerBuilder<byte[], object?>(config)
            .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
            .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
            .SetValueDeserializer(Options.DeserializerFactory(Registry))
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

        consumer.Assign(position);

        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // ReSharper disable once AccessToDisposedClosure
        await foreach (var record in consumer.Records(_ => cancellator.Cancel(), cancellator.Token).ConfigureAwait(false))
            yield return record;

        await consumer.Stop().ConfigureAwait(false);
    }

    public async Task Process(TopicPartitionOffset position, Func<KrimsonRecord, Task> process, CancellationToken cancellationToken) {
        await foreach (var record in Records(position, cancellationToken).ConfigureAwait(false)) {
            await process(record).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() {
        Registry.Dispose();
        return ValueTask.CompletedTask;
    }
}