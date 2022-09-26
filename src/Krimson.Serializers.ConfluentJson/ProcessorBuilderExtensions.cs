using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Processors.Configuration;

namespace Krimson.Serializers.ConfluentJson;

public static class ProcessorBuilderExtensions {
    public static KrimsonProcessorBuilder UseJson(
        this KrimsonProcessorBuilder builder,
        ISchemaRegistryClient registry,
        Action<JsonDeserializerConfig>? configureDeserializer = null, 
        Action<JsonSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .Deserializer(() => new JsonDynamicDeserializer(registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))))
            .Serializer(() => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}