using System.Collections.Concurrent;
using System.Runtime.Serialization;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Serializers.ConfluentJson.NJsonSchema;
using Newtonsoft.Json;
using NJsonSchema.Generation;
using static System.Activator;
using static System.Array;

namespace Krimson.Serializers.ConfluentJson;

[PublicAPI]
public class JsonDynamicSerializer : IDynamicSerializer {
    static readonly Type                  ConfluentSerializerType = typeof(JsonSerializer<>);
    static readonly List<SchemaReference> EmptyReferencesList     = new List<SchemaReference>();
    
    public static readonly JsonSerializerConfig DefaultConfig = new() {
        SubjectNameStrategy = SubjectNameStrategy.Record,
        AutoRegisterSchemas = true
    };

    public JsonDynamicSerializer(ISchemaRegistryClient registryClient, JsonSerializerConfig? config = null, JsonSchemaGeneratorSettings? generatorSettings = null) {
        RegistryClient = registryClient;
        Serializers    = new();

        var args = (
            Client: RegistryClient,
            Config: config ?? DefaultConfig,
            GeneratorSettings: generatorSettings ?? new JsonSchemaGeneratorSettings {
                SchemaNameGenerator = SchemaFullNameGenerator.Instance
            }
        );

        GetSerializer = messageType => Serializers.GetOrAdd(
            messageType, 
            static (type, ctx) => CreateInstance(
                ConfluentSerializerType.MakeGenericType(type), 
                ctx.Client, ctx.Config, ctx.GeneratorSettings
            )!,
            args
        );
    }

    public JsonDynamicSerializer(ISchemaRegistryClient registryClient, JsonSchemaGeneratorSettings generatorSettings)
        : this(registryClient, null, generatorSettings) { }
    
    public JsonDynamicSerializer(ISchemaRegistryClient registryClient, JsonSerializerOptions serializerOptions)
        : this(registryClient, null, new JsonSchemaGeneratorSettings().ConfigureSystemJson(serializerOptions)) { }

    public JsonDynamicSerializer(ISchemaRegistryClient registryClient, JsonSerializerSettings serializerSettings)
        : this(registryClient, null, new JsonSchemaGeneratorSettings().ConfigureNewtonsoftJson(serializerSettings)) { }
    
    ISchemaRegistryClient               RegistryClient { get; }
    Func<Type, dynamic>                 GetSerializer  { get; }
    ConcurrentDictionary<Type, dynamic> Serializers    { get; }
    
    public async Task<byte[]> SerializeAsync(object? data, SerializationContext context) {
        if (data is null)
            return Empty<byte>();

        var messageType = data.GetType();

        try {
            var serializer  = GetSerializer(messageType);

            byte[] bytes = await serializer
                .SerializeAsync((dynamic)data, context)
                .ConfigureAwait(false);

            context.Headers.AddSchemaInfo(SchemaType.Json, bytes, messageType);

            return bytes;
        }
        catch (Exception ex) {
            throw new SerializationException($"Failed to serialize message to json: {messageType.FullName}", ex);
        }
    }

    public byte[] Serialize(object? data, SerializationContext context) =>
        SerializeAsync(data, context).ConfigureAwait(false).GetAwaiter().GetResult();
}