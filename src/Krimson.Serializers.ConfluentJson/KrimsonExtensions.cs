// ReSharper disable CheckNamespace

using System.Text.Json;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Serializers.ConfluentJson;
using Newtonsoft.Json;

namespace Krimson;

[PublicAPI]
public static class KrimsonBuilderExtensions {
    static KrimsonBuilder AddSerializer(this KrimsonBuilder builder, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.AddSerializer(registry => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    static KrimsonBuilder AddDeserializer(this KrimsonBuilder builder, Action<JsonDeserializerConfig>? configureDeserializer = null) =>
        builder.AddDeserializer(registry => new JsonDynamicDeserializer(registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))));

    public static KrimsonBuilder UseConfluentJson(this KrimsonBuilder builder, Action<JsonDeserializerConfig>? configureDeserializer = null, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.AddDeserializer(configureDeserializer).AddSerializer(configureSerializer);

     public static KrimsonBuilder UseConfluentSystemJson(this KrimsonBuilder builder, JsonSerializerOptions? serializerOptions = null) =>
        builder
            .AddSerializer(registry => new JsonDynamicSerializer(registry, serializerOptions ?? KrimsonSystemJsonSerializerDefaults.General))
            .AddDeserializer(registry => new JsonDynamicDeserializer(registry, serializerOptions ?? KrimsonSystemJsonSerializerDefaults.General));

     public static KrimsonBuilder UseConfluentNewtonsoftJson(this KrimsonBuilder builder, JsonSerializerSettings? serializerSettings = null) =>
         builder
             .AddSerializer(registry => new JsonDynamicSerializer(registry, serializerSettings ?? new JsonSerializerSettings()))
             .AddDeserializer(registry => new JsonDynamicDeserializer(registry, serializerSettings ?? new JsonSerializerSettings()));
}

public static class ProcessorBuilderExtensions {
    public static KrimsonProcessorBuilder UseConfluentJson(this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry, Action<JsonDeserializerConfig>? configureDeserializer = null, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder
            .Deserializer(() => new JsonDynamicDeserializer(registry, JsonDynamicDeserializer.DefaultConfig.With(x => configureDeserializer?.Invoke(x))))
            .Serializer(() => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    public static KrimsonProcessorBuilder UseConfluentSystemJson(this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry, JsonSerializerOptions serializerOptions) =>
        builder
            .Deserializer(() => new JsonDynamicDeserializer(registry, serializerOptions))
            .Serializer(() => new JsonDynamicSerializer(registry, serializerOptions));

    public static KrimsonProcessorBuilder UseConfluentNewtonsoftJson(this KrimsonProcessorBuilder builder, ISchemaRegistryClient registry, JsonSerializerSettings serializerSettings) =>
        builder
            .Deserializer(() => new JsonDynamicDeserializer(registry, serializerSettings))
            .Serializer(() => new JsonDynamicSerializer(registry, serializerSettings));
}

public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder UseConfluentJson(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, Action<JsonSerializerConfig>? configureSerializer = null) =>
        builder.Serializer(() => new JsonDynamicSerializer(registry, JsonDynamicSerializer.DefaultConfig.With(x => configureSerializer?.Invoke(x))));

    public static KrimsonProducerBuilder UseConfluentSystemJson(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, JsonSerializerOptions serializerOptions) =>
        builder  .Serializer(() => new JsonDynamicSerializer(registry, serializerOptions));

    public static KrimsonProducerBuilder UseConfluentNewtonsoftJson(this KrimsonProducerBuilder builder, ISchemaRegistryClient registry, JsonSerializerSettings serializerSettings) =>
        builder  .Serializer(() => new JsonDynamicSerializer(registry, serializerSettings));
}