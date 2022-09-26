using System.Net;
using System.Text.Json;
using Confluent.Kafka;
using Json.More;
using Json.Schema;
using Json.Schema.Generation;
using Krimson.SchemaRegistry;

namespace Krimson.Serializers.SystemJson; 

public class SystemJsonSerializer<T> : IAsyncSerializer<T> where T : class {
    public SystemJsonSerializer(JsonSerializerOptions? options = null) => 
        Options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

    JsonSerializerOptions Options       { get; }
    JsonSchemaBuilder     SchemaBuilder { get; } = new();
    
    public Task<byte[]> SerializeAsync(T? value, SerializationContext context) {
        
        // this.schema = this.jsonSchemaGeneratorSettings == null
        //     ? NJsonSchema.JsonSchema.FromType<T>()
        //     : NJsonSchema.JsonSchema.FromType<T>(this.jsonSchemaGeneratorSettings);
        // this.schemaFullname = schema.Title;
        // this.schemaText     = schema.ToJson();
        
        
        var schema     = SchemaBuilder.FromType<T>().Build();
        var schemaText = schema.ToJsonDocument(Options).RootElement.ToJsonString();

        var bytes = JsonSerializer.SerializeToUtf8Bytes(value, Options);

        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        stream.WriteByte(Constants.MagicByte);
        writer.Write(IPAddress.HostToNetworkOrder(schemaId.Value));
        writer.Write(bytes);
        return stream.ToArray();

        // var bytes = value is null ? Array.Empty<byte>() : JsonSerializer.SerializeToUtf8Bytes(value, Options);
        // return Task.FromResult(bytes);
    }
}