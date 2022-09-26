using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Producers;

namespace Krimson.Serializers.ConfluentJson;

public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder UseJson(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.Serializer(() => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}