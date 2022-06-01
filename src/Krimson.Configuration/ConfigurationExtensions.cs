using Microsoft.Extensions.Configuration;

namespace Krimson.Configuration;

static class ConfigurationExtensions {
    public static string[] GetValues(this IConfiguration configuration, string key, char separator = ',') {
        var value = configuration.GetValue<string>(key, "");
        
        return string.IsNullOrWhiteSpace(value)
            ? Array.Empty<string>()
            : value.Contains(separator)
                ? value.Split(separator, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                : new[] { value };
    }
}