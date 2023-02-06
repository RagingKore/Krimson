using Confluent.Kafka;
using Krimson.Persistence.Outbox;
using Krimson.Producers;
using Krimson.Serializers;
using MongoDB.Bson;
using MongoDB.Driver;
using OneOf.Types;
using Polly;

namespace Krimson.Persistence.MongoDB.Outbox;

/// <summary>
///     A MongoDB transactional outbox that can be used to push messages to the outbox within a transaction.
/// </summary>
public class MongoOutbox : IOutbox<MongoOutboxOperationContext, IClientSessionHandle> {
    static readonly ILogger Logger = Log.ForContext<MongoOutbox>();

    static readonly AsyncPolicy DefaultOutboxErrorHandlingPolicy = Policy
        .Handle<MongoException>()
        .WaitAndRetryAsync(
            retryCount: 3,
            retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
            (exception, timeSpan, retryCount, context) => {
                Logger.Error(exception, "Error while processing outbox. Retrying in {TimeSpan} (attempt {RetryCount})", timeSpan, retryCount);
            }
        );

    static readonly InsertOneOptions DefaultInsertOneOptions = new() {
        BypassDocumentValidation = false
    };

    static readonly BulkWriteOptions DefaultBulkWriteOptions = new() {
        IsOrdered = true
    };

    static ClientSessionOptions DefaultClientSessionOptions =>
        new() {
            CausalConsistency         = true,
            Snapshot                  = false,
            DefaultTransactionOptions = new(
                ReadConcern.Local, //Snapshot
                ReadPreference.Primary,
                WriteConcern.WMajority
            )
        };

    public MongoOutbox(
        string clientId,
        IMongoDatabase database,
        IDynamicSerializer serializer,
        AsyncPolicy? outboxErrorHandlingPolicy = null
    ) {
        ClientId                  = clientId;
        Database                  = database;
        Serializer                = serializer;
        OutboxMessages            = database.GetCollection<OutboxMessage>($"{clientId}-outbox");
        OutboxErrorHandlingPolicy = outboxErrorHandlingPolicy ?? DefaultOutboxErrorHandlingPolicy;
        NextSequenceNumber        = _ => ValueTask.FromResult(DateTime.Now.Ticks);
    }

    IMongoDatabase                              Database                  { get; }
    IDynamicSerializer                          Serializer                { get; }
    AsyncPolicy                                 OutboxErrorHandlingPolicy { get; }
    IMongoCollection<OutboxMessage>             OutboxMessages            { get; }
    Func<IClientSessionHandle, ValueTask<long>> NextSequenceNumber        { get; }
    string                                      ClientId                  { get; }

    /// <inheritdoc />
    public async Task<OutboxMessage> ProduceToOutbox(ProducerRequest request, IClientSessionHandle session, CancellationToken cancellationToken = default) {
        var outboxMessage = request.ToOutboxMessage(ClientId, await NextSequenceNumber(session), Serializer);

        await OutboxErrorHandlingPolicy.ExecuteAsync(
            ct => OutboxMessages.InsertOneAsync(session, outboxMessage , DefaultInsertOneOptions, ct)
          , cancellationToken
        );

        Logger.Debug(
            "{ClientId} {RequestId} >> ({Id}) @ {SequenceNumber}",
            ClientId, outboxMessage.RequestId, outboxMessage.Id, outboxMessage.SequenceNumber
        );

        return outboxMessage;
    }

    /// <inheritdoc />
    public async Task<OutboxMessage[]> ProduceManyToOutbox(ProducerRequest[] requests, IClientSessionHandle session, CancellationToken cancellationToken = default) {
        var messages = new List<OutboxMessage>();

        var inserts = requests
            .Select(
                request => {
                    var outboxMessage = request.ToOutboxMessage(ClientId, NextSequenceNumber(session).AsTask().GetAwaiter().GetResult(), Serializer);

                    messages.Add(outboxMessage);

                    Logger.Debug(
                        "{ClientId} {RequestId} >> ({Id}) @ {SequenceNumber}",
                        ClientId, outboxMessage.RequestId,  outboxMessage.Id, outboxMessage.SequenceNumber
                    );

                    return new InsertOneModel<OutboxMessage>(outboxMessage);
                }
            )
            .ToList();

        await OutboxErrorHandlingPolicy.ExecuteAsync(
            ct => OutboxMessages.BulkWriteAsync(session, inserts, DefaultBulkWriteOptions, ct)
          , cancellationToken
        );

        return messages.ToArray();
    }

    /// <inheritdoc />
    public async Task<TResult> WithTransaction<TResult>(Func<MongoOutboxOperationContext, Task<TResult>> operation, Func<CancellationToken, Task<IClientSessionHandle>> transactionScopeFactory, CancellationToken cancellationToken) {
        using var session = await transactionScopeFactory(cancellationToken);

        return await session.WithTransactionAsync<TResult>(
            async (clientSessionHandle, ct) => {
                var ctx = new MongoOutboxOperationContext(Database, clientSessionHandle, ct) {
                    ProduceRequestToOutbox = ProduceToOutbox
                };

                return await operation(ctx);
            }
          , session.Options.DefaultTransactionOptions
          , cancellationToken
        );
    }

    /// <inheritdoc />
    public Task<TResult> WithTransaction<TResult>(Func<MongoOutboxOperationContext, Task<TResult>> operation, CancellationToken cancellationToken = default) =>
        WithTransaction(operation, ct => Database.Client.StartSessionAsync(DefaultClientSessionOptions, ct), cancellationToken);

    public async Task<OutboxTransactionResult<TResult>> WithTransactionResult<TResult>(Func<MongoOutboxOperationContext, Task<TResult>> operation, Func<CancellationToken, Task<IClientSessionHandle>> transactionScopeFactory, CancellationToken cancellationToken) {
        using var session = await transactionScopeFactory(cancellationToken);


        return await session.WithTransactionAsync<OutboxTransactionResult<TResult>>(
            async (clientSessionHandle, ct) => {
                var ctx = new MongoOutboxOperationContext(Database, clientSessionHandle, ct) {
                    ProduceRequestToOutbox = ProduceToOutbox
                };

                var result = await operation(ctx);

                return new(result, ctx.Items.ToArray());
            }
          , session.Options.DefaultTransactionOptions
          , cancellationToken
        );
    }

    public Task<OutboxTransactionResult<TResult>> WithTransactionResult<TResult>(Func<MongoOutboxOperationContext, Task<TResult>> operation, CancellationToken cancellationToken = default) =>
        WithTransactionResult(operation, ct => Database.Client.StartSessionAsync(DefaultClientSessionOptions, ct), cancellationToken);

    public Task<OutboxTransactionResult<None>> WithTransactionResult(Func<MongoOutboxOperationContext, Task> operation, Func<CancellationToken, Task<IClientSessionHandle>> transactionScopeFactory, CancellationToken cancellationToken) =>
        WithTransactionResult(
            async context => {
                await operation(context);
                return new None();
            },
            transactionScopeFactory,
            cancellationToken
        );

    public Task<OutboxTransactionResult<None>> WithTransactionResult(Func<MongoOutboxOperationContext, Task> operation, CancellationToken cancellationToken = default) =>
        WithTransactionResult(operation, ct => Database.Client.StartSessionAsync(DefaultClientSessionOptions, ct), cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}


public static class ProducerRequestOutboxExtensions {
    /// <summary>
    ///     Converts an <see cref="ProducerRequest" /> to a <see cref="OutboxMessage" />.
    /// </summary>
    public static OutboxMessage ToOutboxMessage(this ProducerRequest request, string clientId, long sequenceNumber, IDynamicSerializer serializer) {
        var context = new SerializationContext(
            MessageComponentType.Value, request.Topic,
            request.Headers
                .With(x => x.Add(HeaderKeys.ProducerName, clientId))
                .With(x => x.Add(HeaderKeys.RequestId, request.RequestId.ToString()))
                .With(x => x.Add("krimson.outbox.sequence-number", sequenceNumber.ToString()))
                .Encode()
        );

        var data = serializer.Serialize(request.Message, context);

        var outboxMessage = new OutboxMessage {
            Id               = ObjectId.GenerateNewId(),
            ClientId         = clientId,
            SequenceNumber   = sequenceNumber,
            Key              = request.Key,
            Data             = data,
            Headers          = context.Headers.Decode(),
            DestinationTopic = request.Topic!,
            RequestId        = request.RequestId,
            Timestamp        = request.Timestamp.UnixTimestampMs,
            CreatedOn        = DateTimeOffset.UtcNow
        };

        return outboxMessage;
    }
}