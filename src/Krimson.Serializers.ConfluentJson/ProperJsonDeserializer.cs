// // Copyright 2020 Confluent Inc.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// // http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
// //
// // Refer to LICENSE for more information.
//
// using System.Text;
// using Confluent.Kafka;
// using NJsonSchema.Generation;
//
// namespace Confluent.SchemaRegistry.Serdes;
//
// /// <summary>
// ///     (async) JSON deserializer.
// /// </summary>
// /// <remarks>
// ///     Serialization format:
// ///       byte 0:           A magic byte that identifies this as a message with
// ///                         Confluent Platform framing.
// ///       bytes 1-4:        Unique global id of the JSON schema associated with
// ///                         the data (as registered in Confluent Schema Registry),
// ///                         big endian.
// ///       following bytes:  The JSON data (utf8)
// ///
// ///     Internally, uses Newtonsoft.Json for deserialization. Currently,
// ///     no explicit validation of the data is done against the
// ///     schema stored in Schema Registry.
// ///
// ///     Note: Off-the-shelf libraries do not yet exist to enable
// ///     integration of System.Text.Json and JSON Schema, so this
// ///     is not yet supported by the deserializer.
// /// </remarks>
// public class ProperJsonDeserializer<T> : IAsyncDeserializer<T> where T : class {
//     const int HeaderSize = sizeof(int) + sizeof(byte);
//
//     readonly JsonSchemaGeneratorSettings _jsonSchemaGeneratorSettings;
//
//     /// <summary>
//     ///     Initialize a new JsonDeserializer instance.
//     /// </summary>
//     /// <param name="config">
//     ///     Deserializer configuration properties (refer to
//     ///     <see cref="JsonDeserializerConfig" />).
//     /// </param>
//     /// <param name="jsonSchemaGeneratorSettings">
//     ///     JSON schema generator settings.
//     /// </param>
//     public ProperJsonDeserializer(IEnumerable<KeyValuePair<string, string>>? config = null, JsonSchemaGeneratorSettings? jsonSchemaGeneratorSettings = null) {
//         _jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
//
//         if (config is null) return;
//
//         if (config.Any()) throw new ArgumentException($"JsonDeserializer: unknown configuration parameter {config.First().Key}.");
//     }
//
//     /// <summary>
//     ///     Deserialize an object of type <typeparamref name="T"/>
//     ///     from a byte array.
//     /// </summary>
//     /// <param name="data">
//     ///     The raw byte data to deserialize.
//     /// </param>
//     /// <param name="isNull">
//     ///     True if this is a null value.
//     /// </param>
//     /// <param name="context">
//     ///     Context relevant to the deserialize operation.
//     /// </param>
//     /// <returns>
//     ///     A <see cref="System.Threading.Tasks.Task" /> that completes
//     ///     with the deserialized value.
//     /// </returns>
//     public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context) {
//         if (isNull) return Task.FromResult<T>(default);
//
//         try {
//             var array = data.ToArray();
//
//             if (array.Length < 5) throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {array.Length} bytes");
//
//             if (array[0] != Constants.MagicByte)
//                 throw new InvalidDataException(
//                     $"Expecting message {context.Component.ToString()} with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}"
//                 );
//
//             // A schema is not required to deserialize json messages.
//             // TODO: add validation capability.
//
//             using (var stream = new MemoryStream(array, HeaderSize, array.Length - HeaderSize))
//             using (var sr = new StreamReader(stream, Encoding.UTF8)) {
//                 return Task.FromResult(Newtonsoft.Json.JsonConvert.DeserializeObject<T>(sr.ReadToEnd(), _jsonSchemaGeneratorSettings?.ActualSerializerSettings));
//             }
//         }
//         catch (AggregateException e) {
//             throw e.InnerException;
//         }
//     }
// }