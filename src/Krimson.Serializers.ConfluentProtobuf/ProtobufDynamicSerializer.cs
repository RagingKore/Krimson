using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using static System.Activator;
using static System.Array;
using static Confluent.Kafka.Serializers;

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

        // bypass if data is already a byte array
        var messageType = data.GetType();

        if (messageType == typeof(byte[])) {
            // will add type if it's not already there
            return ByteArray.Serialize(
                data.As<byte[]>(),
                context.With(x => x.Headers.AddSchemaMessageType(messageType))
            );
        }

        if (messageType == typeof(ReadOnlyMemory<byte>)) {
            // will add type if it's not already there
            return ByteArray.Serialize(
                data.As<ReadOnlyMemory<byte>>().ToArray(),
                context.With(x => x.Headers.AddSchemaMessageType(messageType))
            );
        }

        try {
            var serializer  = GetSerializer(messageType);

            byte[] bytes = await serializer
                .SerializeAsync((dynamic)data, context)
                .ConfigureAwait(false);

            context.Headers.AddSchemaInfo(SchemaType.Protobuf, bytes, messageType);

            return bytes;
        }
        catch (Exception ex) {
            throw new SerializationException($"Failed to serialize message as protobuf: {messageType.FullName}", ex);
        }
    }

    public byte[] Serialize(object? data, SerializationContext context) =>
        SerializeAsync(data, context).ConfigureAwait(false).GetAwaiter().GetResult();
}