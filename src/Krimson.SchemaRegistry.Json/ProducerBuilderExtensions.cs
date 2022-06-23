using Confluent.SchemaRegistry.Serdes;
using Krimson.Producers;

namespace Krimson.SchemaRegistry.Json;

public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder UseJson(this KrimsonProducerBuilder builder, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.Serializer(registry => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}