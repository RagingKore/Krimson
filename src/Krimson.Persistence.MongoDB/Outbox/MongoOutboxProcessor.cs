using System.Threading.Channels;
using Krimson.Persistence.Outbox;
using Krimson.Producers;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Polly;
using WinstonPuckett.ResultExtensions;

namespace Krimson.Persistence.MongoDB.Outbox;

/// <inheritdoc />
public class MongoOutboxProcessor : IOutboxProcessor {
    static readonly ILogger Logger = Log.ForContext<MongoOutboxProcessor>();

    static readonly AsyncPolicy DefaultOutboxErrorHandlingPolicy = Policy
        .Handle<MongoException>()
        .WaitAndRetryAsync(
            retryCount: 3,
            retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
            (exception, timeSpan, retryCount, context) => {
                Logger.Error(exception, "Error while processing outbox. Retrying in {TimeSpan} (attempt {RetryCount})", timeSpan, retryCount);
            }
        );

    public MongoOutboxProcessor(IMongoDatabase database, KrimsonProducer producer, AsyncPolicy? outboxErrorHandlingPolicy = null) {
        Producer                  = producer;
        OutboxMessages            = database.GetCollection<OutboxMessage>($"{producer.ClientId}-outbox");
        ClientId                  = producer.ClientId;
        OutboxErrorHandlingPolicy = outboxErrorHandlingPolicy ?? DefaultOutboxErrorHandlingPolicy;
    }

    KrimsonProducer                 Producer                  { get; }
    AsyncPolicy                     OutboxErrorHandlingPolicy { get; }
    IMongoCollection<OutboxMessage> OutboxMessages            { get; }
    string                          ClientId                  { get; }

    /// <inheritdoc />
    public IAsyncEnumerable<OutboxMessage> ProcessOutbox(OutboxProcessingStrategy strategy, CancellationTokenSource cancellator) {
        return strategy == OutboxProcessingStrategy.Delete
            ? ProcessOutbox(
                OutboxMessages.AsQueryable()
                    .OrderBy(x => x.ClientId)
                    .ThenBy(x => x.Position)
                    .ToAsyncEnumerable(),
                async outboxMessage => {
                    await OutboxErrorHandlingPolicy.ExecuteAsync(() => OutboxMessages.DeleteOneAsync(Builders<OutboxMessage>.Filter.Eq(x => x.Id, outboxMessage.Id)));

                    Logger.Debug(
                        "{ClientId} {RequestId} << ({Id}) @ {SequenceNumber} | strategy: {Strategy}",
                        ClientId, outboxMessage.RequestId, outboxMessage.Id, outboxMessage.Position, strategy
                    );
                },
                cancellator
            )
            : ProcessOutbox(
                OutboxMessages.AsQueryable()
                    .Where(x => x.ProcessedOn == null)
                    .OrderBy(x => x.ClientId)
                    .ThenBy(x => x.Position)
                    .ToAsyncEnumerable(),
                async outboxMessage => {
                    await OutboxErrorHandlingPolicy.ExecuteAsync(
                        () => OutboxMessages.UpdateOneAsync(
                            Builders<OutboxMessage>.Filter.Eq(x => x.Id, outboxMessage.Id),
                            Builders<OutboxMessage>.Update.Set(x => x.ProcessedOn, DateTimeOffset.UtcNow)
                        )
                    );

                    Logger.Debug(
                        "{ClientId} {RequestId} << ({Id}) @ {SequenceNumber} | strategy: {Strategy}",
                        ClientId, outboxMessage.RequestId, outboxMessage.Id, outboxMessage.Position, strategy
                    );
                },
                cancellator
            );
    }

    IAsyncEnumerable<OutboxMessage> ProcessOutbox(IAsyncEnumerable<OutboxMessage> messages, Func<OutboxMessage, Task> handler, CancellationTokenSource cancellator) {
        var channel = Channel.CreateUnbounded<OutboxMessage>();

        _ = Task.Run(
            async () => {
                try {
                    await messages.ForEachAsync(outboxMessage => Producer.Produce(ToProducerRequest(outboxMessage), OnResult(outboxMessage)), cancellator.Token);
                }
                finally {
                    Producer.Flush();
                    channel.Writer.TryComplete();
                }
            }, cancellator.Token
        );

        return channel.Reader.ReadAllAsync();

        Func<ProducerResult, Task> OnResult(OutboxMessage outboxMessage) {
            return async result => {
                if (result.Success) {
                    await handler(outboxMessage);
                    await channel.Writer.WriteAsync(outboxMessage);
                }
                else {
                    cancellator.Cancel(true);
                    channel.Writer.TryComplete(result.Exception);
                }
            };
        }
    }

    /// <inheritdoc />
    public Task<long> CleanUpOutbox(DateTimeOffset processedUpTo, CancellationToken cancellationToken) =>
        OutboxErrorHandlingPolicy.ExecuteAsync(ct => OutboxMessages.DeleteManyAsync(Builders<OutboxMessage>.Filter.Lt(x => x.ProcessedOn, processedUpTo), ct), cancellationToken)
            .Bind(result => result.DeletedCount).Unwrap();

    /// <inheritdoc />
    public Task<long> CleanUpOutbox(TimeSpan retentionPeriod, CancellationToken cancellationToken) =>
        CleanUpOutbox(DateTimeOffset.UtcNow - retentionPeriod, cancellationToken);

    /// <inheritdoc />
    public Task<long> ClearOutbox(CancellationToken cancellationToken) =>
        OutboxErrorHandlingPolicy.ExecuteAsync(ct => OutboxMessages.DeleteManyAsync(Builders<OutboxMessage>.Filter.Empty, ct), cancellationToken)
            .Bind(result => result.DeletedCount).Unwrap();

    public IAsyncEnumerable<OutboxMessage> LoadOutbox() =>
        OutboxMessages.AsQueryable()
            .OrderBy(x => x.ClientId)
            .ThenBy(x => x.Position)
            .ToAsyncEnumerable();

    public IAsyncEnumerable<OutboxMessage> LoadOutbox(DateTimeOffset processedUpTo) =>
        OutboxMessages.AsQueryable()
            .Where(x => x.ProcessedOn != null || x.ProcessedOn <= processedUpTo)
            .OrderBy(x => x.ClientId)
            .ThenBy(x => x.Position)
            .ToAsyncEnumerable();

    /// <inheritdoc />
    public ValueTask DisposeAsync() {
        Producer.Flush();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Converts an <see cref="OutboxMessage" /> to a <see cref="ProducerRequest" />.
    /// </summary>
    static ProducerRequest ToProducerRequest(OutboxMessage outboxMessage) {
        var request = ProducerRequest.Builder
            .RequestId(outboxMessage.RequestId)
            .Message(outboxMessage.Data)
            .Key(outboxMessage.Key)
            .Headers(outboxMessage.Headers)
            .Topic(outboxMessage.DestinationTopic)
            .Timestamp(outboxMessage.Timestamp)
            .Create();

        return request;
    }
}