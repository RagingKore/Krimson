using System.Text.Json;
using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public static class StructHelperExtensions {
    public static Struct    ToStruct(this JsonNode value, JsonSerializerOptions? options = null) => Struct.Parser.ParseJson(value.ToJsonString(options));
    public static T?        To<T>(this Struct value, JsonSerializerOptions? options = null)      => JsonSerializer.Deserialize<T>(value.ToString(), options);
    public static JsonNode? ToJsonNode(this Struct value, JsonSerializerOptions? options = null) => To<JsonNode>(value, options);
}