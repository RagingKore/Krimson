using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using JetBrains.Annotations;
using Krimson.SchemaRegistry;
using NJsonSchema.Generation;
using static System.Activator;
using static System.Array;

namespace Krimson.Serializers.ConfluentJson;

[PublicAPI]
public class JsonDynamicSerializer : IDynamicSerializer {
    static readonly Type ConfluentSerializerType = typeof(JsonSerializer<>);
    
    public static readonly JsonSerializerConfig DefaultConfig = new() {
        SubjectNameStrategy = SubjectNameStrategy.Record,
        AutoRegisterSchemas = true
    };

    public static readonly JsonSchemaGeneratorSettings DefaultGeneratorSettings = new();

    public JsonDynamicSerializer(ISchemaRegistryClient registryClient, JsonSerializerConfig serializerConfig, JsonSchemaGeneratorSettings generatorSettings) {
        RegistryClient = registryClient;
        Serializers    = new ConcurrentDictionary<Type, dynamic>();
        
        GetSerializer = messageType => Serializers.GetOrAdd(
            messageType, static (type, ctx) => CreateInstance(ConfluentSerializerType.MakeGenericType(type), ctx.Client, ctx.Config, ctx.Settings)!,
            (Client: RegistryClient, Config: serializerConfig, Settings: generatorSettings)
        );
    }

    public JsonDynamicSerializer(ISchemaRegistryClient registryClient, JsonSerializerConfig serializerConfig)
        : this(registryClient, serializerConfig, DefaultGeneratorSettings) { }

    public JsonDynamicSerializer(ISchemaRegistryClient registryClient, JsonSchemaGeneratorSettings generatorSettings)
        : this(registryClient, DefaultConfig, generatorSettings) { }

    public JsonDynamicSerializer(ISchemaRegistryClient registryClient)
        : this(registryClient, DefaultConfig, DefaultGeneratorSettings) { }

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

            return bytes;
        }
        catch (Exception ex) {
            throw new SerializationException("Serialization error!", ex);
        }
    }

    public byte[] Serialize(object? data, SerializationContext context) =>
        SerializeAsync(data, context).ConfigureAwait(false).GetAwaiter().GetResult();
}
