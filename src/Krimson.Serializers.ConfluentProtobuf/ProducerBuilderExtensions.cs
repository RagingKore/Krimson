using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Producers;

namespace Krimson.Serializers.ConfluentProtobuf;

public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder UseProtobuf(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, Action<ProtobufSerializerConfig>? configureSerializer = null) =>
        builder.Serializer(() => new ProtobufDynamicSerializer(registry, ProtobufDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}