using Microsoft.Extensions.Configuration;
using static System.String;

namespace Krimson;

static class ConfigurationExtensions {
    public static string[] Values(this IConfiguration configuration, string key, char separator = ',') {
        Ensure.NotNullOrWhiteSpace(key, nameof(key));
        
        var value = configuration.GetValue(key, Empty);
        
        return IsNullOrWhiteSpace(value)
            ? Array.Empty<string>()
            : value.Contains(separator)
                ? value.Split(separator, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                : new[] { value };
    }
    
    public static T Value<T>(this IConfiguration configuration, string key, T defaultValue) {
        Ensure.NotNullOrWhiteSpace(key, nameof(key));
        return configuration.GetValue(key, defaultValue)!;
    }
}