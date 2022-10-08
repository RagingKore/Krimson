using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Krimson.SchemaRegistry;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public class ProtobufDynamicDeserializer : IDynamicDeserializer {
    static readonly Type ConfluentDeserializerType = typeof(ProtobufDeserializer<>);

    public static readonly ProtobufDeserializerConfig DefaultConfig = new();

    public ProtobufDynamicDeserializer(ISchemaRegistryClient registryClient, Func<MessageSchema, Type> resolveMessageType, ProtobufDeserializerConfig deserializerConfig) {
        ResolveMessageType = resolveMessageType;
        Deserializers      = new();

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
        if (isNull || data.IsEmpty)
            return null;
        
        if (data.Length < 6)
            throw new InvalidDataException($"Expecting data framing of length 6 bytes or more but total data size is {data.Length} bytes");
        
        try {
            var headers = context.Headers.Decode();

            var messageTypeFromHeader = AppDomain.CurrentDomain
                .GetAssemblies()
                .Select(a => a.GetType(headers[HeaderKeys.SchemaMessageType]!))
                .FirstOrDefault(x => x != null)!;
            
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

    public object? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        DeserializeAsync(new(data.ToArray()), isNull, context).ConfigureAwait(false).GetAwaiter().GetResult();
}