using System.Runtime.CompilerServices;
using MongoDB.Driver;

namespace Krimson.Persistence.MongoDB.Outbox;

[PublicAPI]
static class MongoCollectionChangeStreamExtensions {
    public static async IAsyncEnumerable<ChangeStreamDocument<T>> ChangeStream<T>(
        this IMongoCollection<T> source,
        PipelineDefinition<ChangeStreamDocument<T>,
        ChangeStreamDocument<T>> pipeline,
        ChangeStreamOptions options,
        TimeSpan delay,
        [EnumeratorCancellation] CancellationToken cancellationToken
    ) {
        using var cursor = await source.WatchAsync(pipeline, options, cancellationToken);

        while (!cancellationToken.IsCancellationRequested) {
            while (await cursor.MoveNextAsync(cancellationToken)) {
                foreach (var doc in cursor.Current) {
                    yield return doc;
                }
            }

            await Tasks.SafeDelay(delay, cancellationToken);
        }
    }

    public static IAsyncEnumerable<ChangeStreamDocument<T>> ChangeStream<T>(
        this IMongoCollection<T> source,
        Predicate<ChangeStreamDocument<T>> filter,
        ChangeStreamOptions options,
        TimeSpan delay,
        CancellationToken cancellationToken
    ) =>
        source.ChangeStream(
            new EmptyPipelineDefinition<ChangeStreamDocument<T>>().Match(change => filter(change)), options, delay,
            cancellationToken
        );

    public static IAsyncEnumerable<ChangeStreamDocument<T>> ChangeStream<T>(
        this IMongoCollection<T> source,
        ChangeStreamOperationType operationType,
        ChangeStreamOptions options,
        TimeSpan delay,
        CancellationToken cancellationToken
    ) =>
        source.ChangeStream(
            new EmptyPipelineDefinition<ChangeStreamDocument<T>>().Match(x => x.OperationType == operationType),
            options, delay, cancellationToken
        );
}