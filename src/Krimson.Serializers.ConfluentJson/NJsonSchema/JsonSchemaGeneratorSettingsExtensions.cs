using Newtonsoft.Json;
using NJsonSchema.Generation;

namespace Krimson.Serializers.ConfluentJson.NJsonSchema;

[PublicAPI]
public static class JsonSchemaGeneratorSettingsExtensions {
    public static JsonSchemaGeneratorSettings ConfigureNewtonsoftJson(this JsonSchemaGeneratorSettings generatorSettings, JsonSerializerSettings? serializerSettings = null) {
        generatorSettings.SchemaNameGenerator = SchemaFullNameGenerator.Instance;
        generatorSettings.SerializerOptions   = null;
        generatorSettings.SerializerSettings  = serializerSettings ?? KrimsonNewtonsoftJsonSerializerDefaults.General;

        return generatorSettings;
    }
}