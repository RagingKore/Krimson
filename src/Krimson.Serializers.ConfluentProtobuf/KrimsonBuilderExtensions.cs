// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry.Serdes;
using Krimson.Serializers.ConfluentProtobuf;

namespace Krimson;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    static KrimsonBuilder AddProtobufSerializer(this KrimsonBuilder builder, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.AddSerializer(registry => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    static KrimsonBuilder AddProtobufDeserializer(this KrimsonBuilder builder, Action<ProtobufDeserializerConfig>? configureDeserializer = null) =>
        builder.AddDeserializer(registry => new ProtobufDynamicDeserializer(registry, ProtobufDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))));

    public static KrimsonBuilder UseProtobuf(
        this KrimsonBuilder builder,
        Action<ProtobufDeserializerConfig>? configureDeserializer = null,
        Action<ProtobufSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .AddProtobufDeserializer(configureDeserializer)
            .AddProtobufSerializer(configureSerializer);
}