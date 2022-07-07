// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry.Serdes;
using Krimson.Serializers.ConfluentProtobuf;

namespace Krimson.Extensions.DependencyInjection;

[PublicAPI]
public static class KrimsonBuilderSerializersExtensions {
   
    static KrimsonBuilder UseProtobufSerializer(this KrimsonBuilder builder, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.AddSerializer(registry => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    static KrimsonBuilder UseProtobufDeserializer(this KrimsonBuilder builder, Action<ProtobufDeserializerConfig>? configureDeserializer = null) =>
        builder.AddDeserializer(registry => new ProtobufDynamicDeserializer(registry, ProtobufDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))));

    public static KrimsonBuilder AddProtobuf(
        this KrimsonBuilder builder,
        Action<ProtobufDeserializerConfig>? configureDeserializer = null,
        Action<ProtobufSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .UseProtobufDeserializer(configureDeserializer)
            .UseProtobufSerializer(configureSerializer);
}