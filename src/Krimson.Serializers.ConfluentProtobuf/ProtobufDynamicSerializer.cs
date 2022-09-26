using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using static System.Activator;
using static System.Array;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public class ProtobufDynamicSerializer : IDynamicSerializer {
    static readonly Type ConfluentSerializerType = typeof(ProtobufSerializer<>);
    
    public static readonly ProtobufSerializerConfig DefaultConfig = new() {
        SubjectNameStrategy = SubjectNameStrategy.Record,
        AutoRegisterSchemas = true
    };
    
    public ProtobufDynamicSerializer(ISchemaRegistryClient registryClient, ProtobufSerializerConfig serializerConfig) {
        RegistryClient = registryClient;
        Serializers    = new();
        
        GetSerializer = messageType => Serializers.GetOrAdd(
            messageType, static (type, ctx) => CreateInstance(ConfluentSerializerType.MakeGenericType(type), ctx.Client, ctx.Config)!,
            (Client: RegistryClient, Config: serializerConfig)
        );
    }

    public ProtobufDynamicSerializer(ISchemaRegistryClient registryClient)
        : this(registryClient, DefaultConfig) { }

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

            context.Headers.AddSchemaId(bytes);
            context.Headers.AddSchemaType(SchemaType.Protobuf);
            
            return bytes;
        }
        catch (Exception ex) {
            throw new SerializationException("Protobuf serialization error!", ex);
        }
    }

    public byte[] Serialize(object? data, SerializationContext context) =>
        SerializeAsync(data, context).ConfigureAwait(false).GetAwaiter().GetResult();
}