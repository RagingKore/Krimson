using System.Text;
using Confluent.Kafka;

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
}