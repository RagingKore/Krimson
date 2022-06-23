using System.Collections.Concurrent;
using Confluent.SchemaRegistry;
using JetBrains.Annotations;

namespace Krimson.SchemaRegistry.Json;

static class Constants {
    /// <summary>
    ///     Magic byte that identifies a message with Confluent Platform framing.
    /// </summary>
    public const byte MagicByte = 0;
}

[PublicAPI]
public static class SchemaRegistryClientJsonExtensions {
    static readonly ConcurrentDictionary<int, MessageSchema> Cache = new();
    
    // public static async Task<FileDescriptorProto> GetSchemaFileDescriptorAsync(this ISchemaRegistryClient client, int schemaId) {
    //     var schema = await client.GetSchemaAsync(schemaId, "serialized").ConfigureAwait(false);
    //     return FileDescriptorProto.Parser.ParseFrom(ByteString.FromBase64(schema.SchemaString));
    // }
    //
    // public static FileDescriptorProto GetSchemaFileDescriptor(this ISchemaRegistryClient client, int schemaId) =>
    //     GetSchemaFileDescriptorAsync(client, schemaId).GetAwaiter().GetResult();
    //
    public static MessageSchema GetJsonMessageSchema(this ISchemaRegistryClient client, ReadOnlyMemory<byte> data) {
        if (data.IsEmpty)
            return MessageSchema.Unknown;

        if (data.Length < 5)
            throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {data.Length} bytes");

        if (data.Span[0] != Constants.MagicByte) {
            throw new InvalidDataException(
                $"Expecting message with Confluent Schema Registry framing. "
              + $"Magic byte was {data.Span[0]}, expecting {Constants.MagicByte}"
            );
        }
        //
        // this.schema = this.jsonSchemaGeneratorSettings == null
        //     ? NJsonSchema.JsonSchema.FromType<T>()
        //     : NJsonSchema.JsonSchema.FromType<T>(this.jsonSchemaGeneratorSettings);
        //
        // this.schemaFullname = schema.Title;
        // this.schemaText     = schema.ToJson();

        // var jsonSchema = this.jsonSchemaGeneratorSettings == null
        //     ? NJsonSchema.JsonSchema.FromType<T>()
        //     : NJsonSchema.JsonSchema.FromType<T>(this.jsonSchemaGeneratorSettings);
        //
       
        return Cache.GetOrAdd(SchemaRegistry.ParseSchemaId(data), static (id, registry) => {

            //var schemaId   = SchemaRegistry.ParseSchemaId(data);
            var schema     = registry.GetSchemaAsync(id).GetAwaiter().GetResult();
            var descriptor = NJsonSchema.JsonSchema.FromJsonAsync(schema.SchemaString).GetAwaiter().GetResult();
       
            var messageType = descriptor.Title;
            // var subjectName = $"{descriptor.Package}.{messageType}";
            // var clrTypeName = $"{descriptor.Options.CsharpNamespace}.{messageType}";

            return new MessageSchema(id, "", "");
        }, client);

    }
}