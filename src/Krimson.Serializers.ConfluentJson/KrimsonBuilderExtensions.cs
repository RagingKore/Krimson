// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry.Serdes;
using Krimson.Serializers.ConfluentJson;

namespace Krimson;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    static KrimsonBuilder AddJsonSerializer(this KrimsonBuilder builder, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.AddSerializer(registry => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    static KrimsonBuilder AddJsonDeserializer(this KrimsonBuilder builder, Action<JsonDeserializerConfig>? configureDeserializer = null) =>
        builder.AddDeserializer(registry => new JsonDynamicDeserializer(registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))));

    public static KrimsonBuilder UseProtobuf(
        this KrimsonBuilder builder,
        Action<JsonDeserializerConfig>? configureDeserializer = null,
        Action<JsonSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .AddJsonDeserializer(configureDeserializer)
            .AddJsonSerializer(configureSerializer);
}