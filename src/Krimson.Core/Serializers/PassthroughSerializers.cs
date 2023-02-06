// using Confluent.Kafka;
// using Krimson.Processors.Configuration;
// using Krimson.Producers;
//
// namespace Krimson.Serializers;
//
// // public class PassThroughUtf8Serializer : IDynamicDeserializer, IDynamicSerializer {
// //     public static readonly PassThroughUtf8Serializer Instance = new PassThroughUtf8Serializer();
// //
// //     static readonly ISerializer<string>   Serializer   = Confluent.Kafka.Serializers.Utf8;
// //     static readonly IDeserializer<string> Deserializer = Confluent.Kafka.Deserializers.Utf8;
// //
// //     public Task<object?> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context) =>
// //         Task.FromResult((object?)Deserializer.Deserialize(data.ToArray(), isNull, context));
// //
// //     public object? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
// //         Deserializer.Deserialize(data, isNull, context);
// //
// //     public byte[] Serialize(object? data, SerializationContext context) =>
// //         Serializer.Serialize(data.MaybeAs<string>() ?? string.Empty, context);
// // }
//
// public class PassThroughBytesSerializer : IDynamicDeserializer, IDynamicSerializer {
//     public static readonly PassThroughBytesSerializer Instance = new PassThroughBytesSerializer();
//
//     static readonly ISerializer<byte[]>   Serializer   = Confluent.Kafka.Serializers.ByteArray;
//     static readonly IDeserializer<byte[]> Deserializer = Confluent.Kafka.Deserializers.ByteArray;
//
//     public Task<object?> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context) =>
//         Task.FromResult((object?)Deserializer.Deserialize(data.ToArray(), isNull, context));
//
//     public object? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) {
//         // var headers = context.Headers.Decode();
//         //
//         // var messageTypeFromHeader = AppDomain.CurrentDomain
//         //     .GetAssemblies()
//         //     .Select(a => a.GetType(headers[HeaderKeys.SchemaMessageType]!))
//         //     .FirstOrDefault(x => x != null)!;
//
//         return Deserializer.Deserialize(data, isNull, context);
//     }
//
//     public byte[] Serialize(object? data, SerializationContext context) {
//         if (data is null)
//             return Array.Empty<byte>();
//
//         var messageType = data.GetType();
//
//         context.Headers.AddSchemaMessageType(messageType);
//
//         return Serializer.Serialize(data.MaybeAs<byte[]>() ?? Array.Empty<byte>(), context);
//     }
// }
//
// public static class ProducerBuilderExtensions {
//     // public static KrimsonProducerBuilder UseUtf8(this KrimsonProducerBuilder builder) =>
//     //     builder.Serializer(() => new PassThroughUtf8Serializer());
//
//     public static KrimsonProducerBuilder UseBytes(this KrimsonProducerBuilder builder) =>
//         builder.Serializer(() => new PassThroughBytesSerializer());
// }
//
// public static class ProcessorBuilderExtensions {
//     // public static KrimsonProcessorBuilder UseUtf8(this KrimsonProcessorBuilder builder) =>
//     //     builder.Deserializer(() => PassThroughUtf8Serializer.Instance).Serializer(() => PassThroughUtf8Serializer.Instance);
//
//     public static KrimsonProcessorBuilder UseBytes(this KrimsonProcessorBuilder builder) =>
//         builder.Deserializer(() => PassThroughBytesSerializer.Instance).Serializer(() => PassThroughBytesSerializer.Instance);
// }
//
// [PublicAPI]
// public static class KrimsonBuilderExtensions {
//     // public static KrimsonBuilder UseUtf8(this KrimsonBuilder builder) =>
//     //     builder.AddSerializer(_ => PassThroughUtf8Serializer.Instance).AddDeserializer(_ => PassThroughUtf8Serializer.Instance);
//
//     public static KrimsonBuilder UseBytes(this KrimsonBuilder builder) =>
//         builder.AddSerializer(_ => PassThroughBytesSerializer.Instance).AddDeserializer(_ => PassThroughBytesSerializer.Instance);
// }