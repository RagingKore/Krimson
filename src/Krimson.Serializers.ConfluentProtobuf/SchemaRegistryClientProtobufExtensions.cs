using System.Buffers;
using System.Collections.Concurrent;
using Confluent.SchemaRegistry;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using ImTools;
using Krimson.SchemaRegistry;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public static class SchemaRegistryClientProtobufExtensions {
    // static readonly ImHashMap<int, MessageSchema> Cache = ImHashMap<int, MessageSchema>.Empty;
   
    static readonly ConcurrentDictionary<int, MessageSchema> Cache = new();

    
    public static async Task<FileDescriptorProto> GetSchemaFileDescriptorAsync(this ISchemaRegistryClient client, int schemaId) {
        var schema = await client.GetSchemaAsync(schemaId, "serialized").ConfigureAwait(false);
        return FileDescriptorProto.Parser.ParseFrom(ByteString.FromBase64(schema.SchemaString));
    }

    public static FileDescriptorProto GetSchemaFileDescriptor(this ISchemaRegistryClient client, int schemaId) =>
        GetSchemaFileDescriptorAsync(client, schemaId).GetAwaiter().GetResult();
    
    public static MessageSchema GetProtobufMessageSchema(this ISchemaRegistryClient client, ReadOnlyMemory<byte> data) {
        if (data.IsEmpty)
            return MessageSchema.Unknown;

        if (data.Length < 6)
            throw new InvalidDataException($"Expecting data framing of length 6 bytes or more but total data size is {data.Length} bytes");
        
        var messageIndex = GetSchemaMessageIndex(data);
        
        return Cache.GetOrAdd(
            KrimsonSchemaRegistry.ParseSchemaId(data), 
            static (schemaId, ctx) => GetMessageSchema(schemaId, ctx.messageIndex, ctx.client),
            (client, messageIndex)
        );

        static MessageSchema GetMessageSchema(int schemaId, int messageIndex, ISchemaRegistryClient client) {
            var descriptor  = GetSchemaFileDescriptor(client, schemaId);
            var messageType = descriptor.MessageType[messageIndex].Name; //.FirstOrDefault()?.Name; // not possible right?
            var subjectName = $"{descriptor.Package}.{messageType}";
            var clrTypeName = $"{descriptor.Options.CsharpNamespace}.{messageType}";

            return new(schemaId, subjectName, clrTypeName);
        }
    }
    
    public static int GetSchemaMessageIndex(ReadOnlyMemory<byte> data) {
        var reader = new SequenceReader<byte>(new(data));
        
        reader.Advance(5); // jump magic byte and schema id
        
        // Read the index array length, then all of the indices.
        var indicesLength = reader.ReadVarInt();
        
        var messageIndex = 0;
        for (var i = 0; i < indicesLength; ++i)
            messageIndex = reader.ReadVarInt();

        return messageIndex;
    }
}