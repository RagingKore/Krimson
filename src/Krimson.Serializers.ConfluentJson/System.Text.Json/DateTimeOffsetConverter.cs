#pragma warning disable CS8073

// ReSharper disable ConditionIsAlwaysTrueOrFalse
// ReSharper disable CheckNamespace

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Krimson.Serializers.ConfluentJson;

public class DateTimeOffsetConverter : JsonConverter<DateTimeOffset> {
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
        try {
            return DateTimeOffset.Parse(reader.GetString()!);
        }
        catch (Exception ex) {
            throw new Exception("Unable to deserialize the returned DateDateTimeOffset.", ex);
        }
    }
    public override void Write(Utf8JsonWriter writer, DateTimeOffset dateTimeOffsetValue, JsonSerializerOptions options) {

        if (dateTimeOffsetValue != null) {
            // use the serializer's native implementation with ISO 8601-1:2019 format support(and also faster)
            JsonSerializer.Serialize(writer, dateTimeOffsetValue, typeof(DateTimeOffset));
        }
        else {
            throw new Exception("DateTimeOffsetConverter can only serialize objects of type DateTimeOffset.");
        }
    }
}