// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Serializers.ConfluentJson;
using Krimson.Serializers.ConfluentJson.NJsonSchema;
using Newtonsoft.Json;
using NJsonSchema.Generation;

namespace Krimson;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    public static KrimsonBuilder UseConfluentJson(this KrimsonBuilder builder, Action<JsonDeserializerConfig>? configureDeserializer = null, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder
            .AddDeserializer(
                registry => new JsonDynamicDeserializer(
                    registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x)), new JsonSchemaGeneratorSettings().ConfigureNewtonsoftJson()
                )
            )
            .AddSerializer(
                registry => new JsonDynamicSerializer(
                    registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x)), new JsonSchemaGeneratorSettings().ConfigureNewtonsoftJson()
                )
            );

    public static KrimsonBuilder UseConfluentJson(this KrimsonBuilder builder, JsonSerializerSettings? serializerSettings = null) =>
        builder
            .AddSerializer(registry => new JsonDynamicSerializer(registry, serializerSettings ?? KrimsonNewtonsoftJsonSerializerDefaults.General))
            .AddDeserializer(registry => new JsonDynamicDeserializer(registry, serializerSettings ?? KrimsonNewtonsoftJsonSerializerDefaults.General));
}

public static class ProcessorBuilderExtensions {
    public static KrimsonProcessorBuilder UseConfluentJson(
        this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry, Action<JsonDeserializerConfig>? configureDeserializer = null,
        Action<JsonSerializerConfig>? configureSerializer = null
    ) =>
        builder
            .Deserializer(
                () => new JsonDynamicDeserializer(
                    registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x)), new JsonSchemaGeneratorSettings().ConfigureNewtonsoftJson()
                )
            )
            .Serializer(
                () => new JsonDynamicSerializer(
                    registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x)), new JsonSchemaGeneratorSettings().ConfigureNewtonsoftJson()
                )
            );

    public static KrimsonProcessorBuilder UseConfluentJson(this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry, JsonSerializerSettings serializerSettings) =>
        builder
            .Deserializer(() => new JsonDynamicDeserializer(registry, serializerSettings))
            .Serializer(() => new JsonDynamicSerializer(registry, serializerSettings));
}

public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder UseConfluentJson(
        this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, Action<JsonSerializerConfig>? configureSerializer = null
    ) =>
        builder.Serializer(
            () => new JsonDynamicSerializer(
                registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x)), new JsonSchemaGeneratorSettings().ConfigureNewtonsoftJson()
            )
        );

    public static KrimsonProducerBuilder UseConfluentJson(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, JsonSerializerSettings serializerSettings) =>
        builder.Serializer(() => new JsonDynamicSerializer(registry, serializerSettings));
}