using System.Collections.Concurrent;
using Confluent.SchemaRegistry;
using Krimson.SchemaRegistry;
using NJsonSchema;

namespace Krimson.Serializers.ConfluentJson;

[PublicAPI]
public static class SchemaRegistryClientJsonExtensions {
    static readonly ConcurrentDictionary<int, MessageSchema> Cache = new();
    
    public static MessageSchema GetJsonMessageSchema(this ISchemaRegistryClient client, ReadOnlyMemory<byte> data) {
        if (data.IsEmpty)
            return MessageSchema.Unknown;
    
        return Cache.GetOrAdd(
            KrimsonSchemaRegistry.ParseSchemaId(data), 
            static (schemaId, registryClient) => AddMessageSchema(schemaId, registryClient), 
            client
        );
    
        static MessageSchema AddMessageSchema(int schemaId, ISchemaRegistryClient client) {
            var schema     = client.GetSchemaAsync(schemaId).GetAwaiter().GetResult();
            var jsonSchema = JsonSchema.FromJsonAsync(schema.SchemaString).GetAwaiter().GetResult();
            
            return new(schemaId, jsonSchema.Title, jsonSchema.Title);
        }
    }
}