using System.Collections.Concurrent;
using Confluent.SchemaRegistry;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Krimson.SchemaRegistry;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public static class SchemaRegistryClientProtobufExtensions {
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

        return Cache.GetOrAdd(
            KrimsonSchemaRegistry.ParseSchemaId(data), 
            static (schemaId, registryClient) => AddMessageSchema(schemaId, registryClient), 
            client
        );

        static MessageSchema AddMessageSchema(int schemaId, ISchemaRegistryClient client) {
            var descriptor  = GetSchemaFileDescriptor(client, schemaId);
            var messageType = descriptor.MessageType.FirstOrDefault()?.Name; // not possible right?
            var subjectName = $"{descriptor.Package}.{messageType}";
            var clrTypeName = $"{descriptor.Options.CsharpNamespace}.{messageType}";

            return new(schemaId, subjectName, clrTypeName);
        }
    }
}