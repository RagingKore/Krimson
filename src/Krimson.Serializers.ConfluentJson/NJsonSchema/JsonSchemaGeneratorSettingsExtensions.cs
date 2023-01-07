using System.Text.Json;
using Newtonsoft.Json;
using NJsonSchema.Generation;

namespace Krimson.Serializers.ConfluentJson.NJsonSchema;

[PublicAPI]
public static class JsonSchemaGeneratorSettingsExtensions {
    // public static JsonSchemaGeneratorSettings UseSystemJson(this JsonSchemaGeneratorSettings settings, JsonSerializerOptions? options = null) {
    //     // disable Newtonsoft Json
    //     settings.SerializerSettings = null;
    //
    //     // enable System Json
    //     settings.SerializerOptions ??= options ?? KrimsonSystemJsonSerializerDefaults.General;
    //
    //     return settings;
    // }
    
    public static JsonSchemaGeneratorSettings ConfigureSystemJson(this JsonSchemaGeneratorSettings? generatorSettings, JsonSerializerOptions? serializerOptions = null) {
        generatorSettings ??= new() {
            SchemaNameGenerator = SchemaFullNameGenerator.Instance,
        };

        generatorSettings.SerializerSettings = null;
        generatorSettings.SerializerOptions  = serializerOptions ?? KrimsonSystemJsonSerializerDefaults.General;

        return generatorSettings;
    }

    public static JsonSchemaGeneratorSettings ConfigureNewtonsoftJson(this JsonSchemaGeneratorSettings? generatorSettings, JsonSerializerSettings? serializerSettings = null) {
        generatorSettings ??= new() {
            SchemaNameGenerator = SchemaFullNameGenerator.Instance
        };

        generatorSettings.SerializerSettings = serializerSettings ?? new JsonSerializerSettings();

        return generatorSettings;
    }
}