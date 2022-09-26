using System.Text;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.SchemaRegistry;

namespace Krimson;

public delegate string? DecodeHeaderValue(ReadOnlySpan<byte> data);

public delegate ReadOnlySpan<byte> EncodeHeaderValue(string? value);

[PublicAPI]
public static class HeadersExtensions {
    public static Headers Encode(this IDictionary<string, string?> decodedHeaders, EncodeHeaderValue encode) {
        Ensure.NotNull(decodedHeaders, nameof(decodedHeaders));
        Ensure.NotNull(encode, nameof(encode));

        return decodedHeaders.Aggregate(new Headers(), (headers, entry) => {
            try {
                return headers.With(x=> x.Add(entry.Key, encode(entry.Value).ToArray()));
            }
            catch (Exception ex) {
                throw new Exception($"Failed to encode header [{entry.Key}]", ex);
            }
        });
    }

    public static Headers Encode(this IDictionary<string, string?> decodedHeaders) =>
        Encode(
            decodedHeaders, value =>
                value is null 
                    ? Array.Empty<byte>() 
                    : Encoding.UTF8.GetBytes(value)
        );

    public static IDictionary<string, string?> Decode(this Headers? encodedHeaders, DecodeHeaderValue decode) {
        return encodedHeaders.EmptyIfNull().Aggregate(
            new Dictionary<string, string?>(), (headers, entry) => {
                try {
                    return headers.With(x => x.Add(entry.Key, decode(entry.GetValueBytes())));
                }
                catch (Exception ex) {
                    throw new Exception($"Failed to decode header [{entry.Key}]", ex);
                }
            }
        );
    }

    public static IDictionary<string, string?> Decode(this Headers? headers) => Decode(headers, Encoding.UTF8.GetString);
    
    public static Headers Add(this Headers headers, string key, string? value) {
        try {
            var encodedValue = value is null
                ? Array.Empty<byte>()
                : Encoding.UTF8.GetBytes(value);

            headers.Add(key, encodedValue);
            
            return headers;
        }
        catch (Exception ex) {
            throw new Exception($"Failed to encode header [{key}]", ex);
        }
    }

    public static Headers AddSchemaType(this Headers headers, SchemaType schemaType) => 
        headers.Add(HeaderKeys.SchemaType, schemaType.ToString().ToLower());
    
    public static Headers AddSchemaId(this Headers headers, byte[] bytes) => 
        headers.Add(HeaderKeys.SchemaId, KrimsonSchemaRegistry.ParseSchemaId(bytes).ToString());
    
    public static Headers AddSchemaInfo(this Headers headers, SchemaType schemaType, byte[] bytes) => 
        headers.AddSchemaType(schemaType).AddSchemaId(bytes);
}