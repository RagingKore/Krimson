// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Serializers.ConfluentJson;

namespace Krimson;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    static KrimsonBuilder AddSerializer(this KrimsonBuilder builder, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.AddSerializer(registry => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    static KrimsonBuilder AddDeserializer(this KrimsonBuilder builder, Action<JsonDeserializerConfig>? configureDeserializer = null) =>
        builder.AddDeserializer(registry => new JsonDynamicDeserializer(registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))));

    public static KrimsonBuilder UseConfluentJson(this KrimsonBuilder builder, Action<JsonDeserializerConfig>? configureDeserializer = null, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.AddDeserializer(configureDeserializer).AddSerializer(configureSerializer);
}

public static class ProcessorBuilderExtensions {
    public static KrimsonProcessorBuilder UseConfluentJson(this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry, Action<JsonDeserializerConfig>? configureDeserializer = null, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder
            .Deserializer(() => new JsonDynamicDeserializer(registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))))
            .Serializer(() => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}

public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder UseConfluentJson(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.Serializer(() => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));
}