using System.Collections.Concurrent;
using System.Runtime.Serialization;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.Serializers.ConfluentJson.NJsonSchema;
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
            GeneratorSettings: generatorSettings.ConfigureDefaults()
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
        : this(registryClient, null, new JsonSchemaGeneratorSettings().ConfigureDefaults(serializerOptions)) { }
    
    ISchemaRegistryClient               RegistryClient { get; }
    Func<Type, dynamic>                 GetSerializer  { get; }
    ConcurrentDictionary<Type, dynamic> Serializers    { get; }
    
    public async Task<byte[]> SerializeAsync(object? data, SerializationContext context) {
        if (data is null)
            return Empty<byte>();

        try {
            var serializer = GetSerializer(data.GetType());

            byte[] bytes = await serializer
                .SerializeAsync((dynamic)data, context)
                .ConfigureAwait(false);

            context.Headers.AddSchemaInfo(SchemaType.Json, bytes, GetType());

            return bytes;
        }
        catch (Exception ex) {
            throw new SerializationException("Serialization error!", ex);
        }
    }

    public byte[] Serialize(object? data, SerializationContext context) =>
        SerializeAsync(data, context).ConfigureAwait(false).GetAwaiter().GetResult();
}