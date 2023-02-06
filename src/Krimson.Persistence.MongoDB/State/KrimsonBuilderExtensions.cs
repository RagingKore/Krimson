// ReSharper disable CheckNamespace

namespace Krimson;

public static class MongoStateKrimsonBuilderExtensions {
    public static KrimsonBuilder AddMongoStateStore(this KrimsonBuilder builder, string? defaultCollectionName = null) {
        builder.Services.AddKrimsonMongoStateStore(defaultCollectionName);
        return builder;
    }
}