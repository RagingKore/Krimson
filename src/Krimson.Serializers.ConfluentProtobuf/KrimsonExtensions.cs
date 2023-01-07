// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Serializers.ConfluentProtobuf;

namespace Krimson;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    static KrimsonBuilder AddSerializer(this KrimsonBuilder builder, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.AddSerializer(registry => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    static KrimsonBuilder AddDeserializer(this KrimsonBuilder builder, Action<ProtobufDeserializerConfig>? configureDeserializer = null) =>
        builder.AddDeserializer(registry => new ProtobufDynamicDeserializer(registry, ProtobufDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))));

    public static KrimsonBuilder UseProtobuf(this KrimsonBuilder builder, Action<ProtobufDeserializerConfig>? configureDeserializer = null, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.AddDeserializer(configureDeserializer).AddSerializer(configureSerializer);

    public static KrimsonBuilder UseConfluentProtobuf(this KrimsonBuilder builder, Action<ProtobufDeserializerConfig>? configureDeserializer = null, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.AddDeserializer(configureDeserializer).AddSerializer(configureSerializer);
}

public static class ProcessorBuilderExtensions {
    public static KrimsonProcessorBuilder UseProtobuf(
        this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry,
        Action<ProtobufDeserializerConfig>? configureDeserializer = null, 
        Action<ProtobufSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .Deserializer(() => new ProtobufDynamicDeserializer(registry, ProtobufDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))))
            .Serializer(() => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    public static KrimsonProcessorBuilder UseConfluentProtobuf(
        this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry,
        Action<ProtobufDeserializerConfig>? configureDeserializer = null,
        Action<ProtobufSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .Deserializer(() => new ProtobufDynamicDeserializer(registry, ProtobufDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))))
            .Serializer(() => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}

public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder UseProtobuf(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.Serializer(() => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    public static KrimsonProducerBuilder UseConfluentProtobuf(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.Serializer(() => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}