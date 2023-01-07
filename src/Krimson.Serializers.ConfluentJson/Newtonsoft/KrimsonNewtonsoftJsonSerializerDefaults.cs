// ReSharper disable CheckNamespace

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace Krimson.Serializers.ConfluentJson;

public static class KrimsonNewtonsoftJsonSerializerDefaults {
    public static readonly JsonSerializerSettings General = new JsonSerializerSettings {
        ContractResolver     = new CamelCasePropertyNamesContractResolver(),
        NullValueHandling    = NullValueHandling.Ignore,
        DefaultValueHandling = DefaultValueHandling.Include,
        Converters           = { new StringEnumConverter(new CamelCaseNamingStrategy()) }
    };
}