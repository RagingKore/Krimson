using System.Buffers;
using System.Collections.Concurrent;
using Confluent.SchemaRegistry;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Krimson.SchemaRegistry;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public static class SchemaRegistryClientProtobufExtensions {
    static readonly ConcurrentDictionary<int, MessageSchema> Cache = new();
    
    public static async IAsyncEnumerable<MessageSchema> GetAllSchemas(this ISchemaRegistryClient client, params string[] subjectPrefixes) {
        var subjects = await client.GetAllSubjectsAsync().ConfigureAwait(false);

        foreach (var subject in subjects.Where(x => subjectPrefixes.Any(x.StartsWith))) {
            var versions         = await client.GetSubjectVersionsAsync(subject);
            var registeredSchema = await client.GetRegisteredSchemaAsync(subject, versions.Max());

            if (registeredSchema.SchemaType != SchemaType.Protobuf) continue;

            var descriptor = await client.GetSchemaFileDescriptorAsync(registeredSchema.Id);

            var clrTypeName = $"{descriptor.Options.CsharpNamespace}.{subject[(descriptor.Package.Length + 1)..]}"; // includes the last dot

            yield return new(registeredSchema.Id, subject, clrTypeName, registeredSchema.Version);
        }
    }

    public static async Task PreloadSchemas(this ISchemaRegistryClient client, params string[] subjectPrefixes) {
        var schemas = await client.GetAllSchemas("krimson.tests").ToListAsync().ConfigureAwait(false);
        
        foreach (var schema in schemas) Cache[schema.SchemaId] = schema;
    }

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
            var messageType = descriptor.MessageType[messageIndex].Name;
            var subjectName = $"{descriptor.Package}.{messageType}";
            var clrTypeName = $"{descriptor.Options.CsharpNamespace}.{messageType}";

            return new(schemaId, subjectName, clrTypeName);
        }
        
        static int GetSchemaMessageIndex(ReadOnlyMemory<byte> data) {
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
}