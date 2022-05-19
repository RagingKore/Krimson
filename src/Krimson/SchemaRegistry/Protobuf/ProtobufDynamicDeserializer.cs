using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Krimson.SchemaRegistry.Protobuf;

[PublicAPI]
public class ProtobufDynamicDeserializer : IDynamicDeserializer {
    static readonly Type ConfluentDeserializerType = typeof(ProtobufDeserializer<>);

    public static readonly ProtobufDeserializerConfig DefaultConfig = new();

    public ProtobufDynamicDeserializer(
        ISchemaRegistryClient registryClient,
        Func<MessageSchema, Type> resolveMessageType,
        ProtobufDeserializerConfig deserializerConfig
    ) {
        ResolveMessageType = resolveMessageType;
        Deserializers      = new ConcurrentDictionary<Type, dynamic>();

        GetDeserializer = messageType => Deserializers.GetOrAdd(
            messageType,
            static (type, config) => Activator.CreateInstance(ConfluentDeserializerType.MakeGenericType(type), config)!,
            deserializerConfig
        );
        
        GetMessageSchema = registryClient.GetProtobufMessageSchema; 
    }

    public ProtobufDynamicDeserializer(ISchemaRegistryClient registryClient, Func<MessageSchema, Type> resolveMessageType)
        : this(registryClient, resolveMessageType, DefaultConfig) { }

    public ProtobufDynamicDeserializer(ISchemaRegistryClient registryClient)
        : this(registryClient, schema => {
            return AppDomain.CurrentDomain
                .GetAssemblies()
                .Select(a => a.GetType(schema.ClrTypeName))
                .FirstOrDefault(x => x != null)!;
        }, DefaultConfig) { }

    public ProtobufDynamicDeserializer(ISchemaRegistryClient registryClient, ProtobufDeserializerConfig deserializerConfig)
        : this(registryClient, schema => {
            return AppDomain.CurrentDomain
                .GetAssemblies()
                .Select(a => a.GetType(schema.ClrTypeName))
                .FirstOrDefault(x => x != null)!;
        }, deserializerConfig) { }
    
    Func<ReadOnlyMemory<byte>, MessageSchema> GetMessageSchema   { get; }
    Func<MessageSchema, Type>                 ResolveMessageType { get; }
    Func<Type, dynamic>                       GetDeserializer    { get; }
    ConcurrentDictionary<Type, dynamic>       Deserializers      { get; }

    public async Task<object?> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context) {
        if (isNull)
            return null;

        if (data.IsEmpty)
            return null;

        try {
            var messageSchema = GetMessageSchema(data);
            var messageType   = ResolveMessageType(messageSchema);
            var deserializer  = GetDeserializer(messageType);

            var message = await deserializer
                .DeserializeAsync(data, isNull, context)
                .ConfigureAwait(false);

            return message;
        }
        catch (Exception ex) {
            throw new SerializationException("Deserialization error!", ex);
        }
    }

    public object? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => DeserializeAsync(new ReadOnlyMemory<byte>(data.ToArray()), isNull, context)
            .ConfigureAwait(false).GetAwaiter().GetResult();
}