using System.Text.Json;
using NJsonSchema.Generation;

namespace Krimson.Serializers.ConfluentJson.NJsonSchema;

[PublicAPI]
public static class JsonSchemaGeneratorSettingsExtensions {
    static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);
    
    static readonly JsonSchemaGeneratorSettings DefaultGeneratorSettings = new JsonSchemaGeneratorSettings {
        SerializerSettings  = null,
        SerializerOptions   = DefaultJsonSerializerOptions,
        SchemaNameGenerator = SchemaFullNameGenerator.Instance
    };

    public static JsonSchemaGeneratorSettings UseSystemJson(this JsonSchemaGeneratorSettings settings, JsonSerializerOptions? options = null) {
        // disable Newtonsoft Json
        settings.SerializerSettings = null;
        
        // enable System Json
        settings.SerializerOptions ??= options ?? DefaultJsonSerializerOptions;
        
        return settings;
    }
    
    public static JsonSchemaGeneratorSettings ConfigureDefaults(this JsonSchemaGeneratorSettings? settings, JsonSerializerOptions? options = null) {
        if (settings is null)
            settings = DefaultGeneratorSettings;
        else {
            settings.SchemaNameGenerator = SchemaFullNameGenerator.Instance;
            settings.UseSystemJson(options);
        }
        
        return settings;
    }
}