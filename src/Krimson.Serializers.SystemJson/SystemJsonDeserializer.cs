using System.Collections.Concurrent;
using System.Runtime.Serialization;
using System.Text.Json;
using Confluent.Kafka;
using Krimson.SchemaRegistry;

namespace Krimson.Serializers.SystemJson;

public class SystemJsonDeserializer<T> : IAsyncDeserializer<T> where T : class {
    const int HeaderSize = sizeof(int) + sizeof(byte);
    
    public SystemJsonDeserializer(JsonSerializerOptions? options = null) => 
        Options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

    JsonSerializerOptions Options { get; }
    
    public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context) {
        if (isNull || data.IsEmpty) return Task.FromResult<T>(null!);

        try {
            // skip to message bytes if the message is a confluent json message
            return data.Span[0] is Constants.MagicByte && data.Length > 5
                ? Task.FromResult(JsonSerializer.Deserialize<T>(data[HeaderSize..].Span, Options)!)
                : Task.FromResult(JsonSerializer.Deserialize<T>(data.Span, Options)!);
        }
        catch (AggregateException ex) {
            throw ex.InnerException!;
        }
    }
}


[PublicAPI]
public class SystemJsonDynamicDeserializer : IDynamicDeserializer {
    static readonly Type ConfluentDeserializerType = typeof(SystemJsonDeserializer<>);

    public SystemJsonDynamicDeserializer(JsonSerializerOptions? options = null) {
        Options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
        
        Deserializers = new();

        
        GetDeserializer = messageType => Deserializers.GetOrAdd(
            messageType,
            static (type, options) => Activator.CreateInstance(
                ConfluentDeserializerType.MakeGenericType(type),
                options
            )!, 
            Options
        );
    }

    JsonSerializerOptions                     Options            { get; }
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
        => DeserializeAsync(new(data.ToArray()), isNull, context)
            .ConfigureAwait(false).GetAwaiter().GetResult();
}