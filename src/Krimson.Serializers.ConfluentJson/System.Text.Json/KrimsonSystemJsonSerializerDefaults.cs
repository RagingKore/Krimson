// ReSharper disable CheckNamespace

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Krimson.Serializers.ConfluentJson;

public static class KrimsonSystemJsonSerializerDefaults {
    public static readonly JsonSerializerOptions General = new JsonSerializerOptions(JsonSerializerDefaults.Web) {
        NumberHandling         = JsonNumberHandling.AllowReadingFromString,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
            new ObjectToInferredTypesConverter(),
            new DateTimeOffsetConverter()
        }
    };
}