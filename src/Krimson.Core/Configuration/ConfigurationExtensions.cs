using Microsoft.Extensions.Configuration;
using static System.String;

namespace Krimson;

static class ConfigurationExtensions {
    public static string[] Values(this IConfiguration configuration, string key, char separator = ',') {
        if (IsNullOrWhiteSpace(key)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(key));

        var value = configuration.GetValue<string>(key, "");
        
        return IsNullOrWhiteSpace(value)
            ? Array.Empty<string>()
            : value.Contains(separator)
                ? value.Split(separator, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                : new[] { value };
    }
    
    public static T Value<T>(this IConfiguration configuration, string key, T defaultValue) {
        if (IsNullOrWhiteSpace(key)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(key));

        return configuration.GetValue(key, defaultValue)!;
    }
}