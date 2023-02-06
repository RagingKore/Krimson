using Krimson.Persistence.Outbox;
using MongoDB.Driver;

namespace Krimson.Persistence.MongoDB.Outbox;

[PublicAPI]
public record MongoOutboxOperationContext(IMongoDatabase Database, IClientSessionHandle TransactionScope, CancellationToken CancellationToken)
    : OutboxOperationContext<IClientSessionHandle>(TransactionScope, CancellationToken);