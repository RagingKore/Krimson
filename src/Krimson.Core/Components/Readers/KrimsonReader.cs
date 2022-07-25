using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Consumers.Interceptors;
using Krimson.Interceptors;
using Krimson.Readers.Configuration;
using Krimson.Readers.Interceptors;
using Serilog;

namespace Krimson.Readers;

public interface IKrimsonReaderInfo {
    string   ClientId         { get; }
    string   BootstrapServers { get; }
}

[PublicAPI]
public sealed class KrimsonReader : IKrimsonReaderInfo {
    public static KrimsonReaderBuilder Builder => new();

    public KrimsonReader(KrimsonReaderOptions options) {
        Options  = options;
        ClientId = $"{options.ConsumerConfiguration.ClientId}-reader";
        Logger   = Log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, ClientId);

        Intercept = options.Interceptors
            .Prepend(new KrimsonReaderLogger().WithName($"KrimsonReader({ClientId})"))
            .Prepend(new ConfluentConsumerLogger())
            .Intercept;
    }

    KrimsonReaderOptions  Options   { get; }
    ILogger               Logger    { get; }
    ISchemaRegistryClient Registry  { get; }
    Intercept             Intercept { get; }

    public string ClientId         { get; }
    public string BootstrapServers { get; }

    public async IAsyncEnumerable<KrimsonRecord> Records(TopicPartitionOffset startPosition, [EnumeratorCancellation] CancellationToken cancellationToken) {
        using var consumer = new ConsumerBuilder<byte[], object?>(Options.ConsumerConfiguration.With(x => x.GroupId = $"{ClientId}-{Guid.NewGuid().ToString("N")[26..]}"))
            .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
            .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
            .SetValueDeserializer(Options.DeserializerFactory())
            .Build();

        consumer.Assign(startPosition);

        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // ReSharper disable once AccessToDisposedClosure
        await foreach (var record in consumer.Records(partitionEndReached: _ => cancellator.Cancel(), cancellator.Token).ConfigureAwait(false))
            yield return record;

        try {
            consumer.Unassign();
            consumer.Close();
        }
        catch (KafkaException kex) when (kex.IsTransient()) {
            // only throw on terminal error
        }
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

    public async Task<List<TopicPartitionOffset>> GetLatestPositions(string topic, CancellationToken cancellationToken = default) {
        // I really really hate this... there must be another way...
        
        using var consumer = new ConsumerBuilder<byte[], object?>(Options.ConsumerConfiguration.With(x => x.GroupId = $"{ClientId}-{Guid.NewGuid().ToString("N")[26..]}"))
            .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
            .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
            .SetValueDeserializer(Options.DeserializerFactory())
            .Build();

        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var positions = await consumer
            .GetTopicLatestPositions(topic, cancellator.Token)
            .ToListAsync(cancellator.Token);

        try {
            consumer.Close();
        }
        catch (KafkaException kex) when (kex.IsTransient()) {
            // only throw on terminal error
        }

        return positions;
    }

    // public async IAsyncEnumerable<KrimsonRecord> LastRecords(string topic, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
    //     var positions = await GetLatestPositions(topic, cancellationToken)
    //         .ConfigureAwait(false);
    //
    //     var lastPositions = positions.Select(x => new TopicPartitionOffset(x.Topic, x.Partition, x.Offset - 1));
    //     
    //     foreach (var position in lastPositions) {
    //         var record = await Records(position, cancellationToken)
    //             .LastAsync(cancellationToken)
    //             .ConfigureAwait(false);
    //
    //         yield return record;
    //     }
    // }
    
    public async IAsyncEnumerable<KrimsonRecord> LastRecords(string topic, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        using var consumer = new ConsumerBuilder<byte[], object?>(Options.ConsumerConfiguration.With(x => x.GroupId = $"{ClientId}-{Guid.NewGuid().ToString("N")[26..]}"))
            .SetLogHandler((csr, log) => Intercept(new ConfluentConsumerLog(ClientId, csr.GetInstanceName(), log)))
            .SetErrorHandler((csr, err) => Intercept(new ConfluentConsumerError(ClientId, csr.GetInstanceName(), err)))
            .SetValueDeserializer(Options.DeserializerFactory())
            .Build();

        using var cancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var positions = await consumer
            .GetTopicLatestPositions(topic, cancellator.Token)
            .ToListAsync(cancellator.Token);
        
        var lastPositions = positions.Select(x => new TopicPartitionOffset(x.Topic, x.Partition, x.Offset - 1));

        foreach (var position in lastPositions) {
            if (position.Offset == Offset.Beginning)
                continue;

            consumer.Assign(position);

            using var recordReaderCancellator = CancellationTokenSource.CreateLinkedTokenSource(cancellator.Token);

            yield return await consumer
                .Records(_ => recordReaderCancellator.Cancel(), cancellator.Token)
                .FirstAsync(recordReaderCancellator.Token)
                .ConfigureAwait(false);

            recordReaderCancellator.Cancel();
            consumer.Unassign();
        }
        
        try {
            consumer.Close();
        }
        catch (KafkaException kex) when (kex.IsTransient()) {
            // only throw on terminal error
        }
    }

    public ValueTask DisposeAsync() {
        return ValueTask.CompletedTask;
    }
}