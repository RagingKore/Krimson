using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Processors.Configuration;

namespace Krimson.Serializers.ConfluentProtobuf;

public static class ProcessorBuilderExtensions {
    public static KrimsonProcessorBuilder UseProtobuf(
        this KrimsonProcessorBuilder builder,
        ISchemaRegistryClient registry,
        Action<ProtobufDeserializerConfig>? configureDeserializer = null, 
        Action<ProtobufSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .Deserializer(() => new ProtobufDynamicDeserializer(registry, ProtobufDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))))
            .Serializer(() => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}