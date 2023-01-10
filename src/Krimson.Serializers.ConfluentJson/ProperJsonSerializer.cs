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
// // Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
//
// #pragma warning disable CS0618
//
// using System.Net;
// using System.Text.Json;
// using Confluent.Kafka;
// using Microsoft.IO;
// using NJsonSchema;
// using NJsonSchema.Generation;
// using NJsonSchema.Validation;
//
// namespace Confluent.SchemaRegistry.Serdes;
//
// /// <summary>
// ///     JSON Serializer.
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
// ///     Internally, the serializer uses Newtonsoft.Json for
// ///     serialization and NJsonSchema for schema creation and
// ///     validation. You can use any property annotations recognised
// ///     by these libraries.
// ///
// ///     Note: Off-the-shelf libraries do not yet exist to enable
// ///     integration of System.Text.Json and JSON Schema, so this
// ///     is not yet supported by the serializer.
// /// </remarks>
// public class ProperJsonSerializer<T> : IAsyncSerializer<T> where T : class {
//     static readonly SubjectNameStrategyDelegate GetSubjectNameAsRecord = SubjectNameStrategy.Record.ToDelegate();
//
//     readonly SemaphoreSlim         SerializeMutex      = new(1);
//
//     readonly List<SchemaReference> EmptyReferencesList = new();
//     readonly HashSet<string>       SubjectsRegistered  = new();
//
//
//     public ProperJsonSerializer(ISchemaRegistryClient schemaRegistryClient, JsonSerializerConfig config, JsonSchemaGeneratorSettings schemaGeneratorSettings) {
//         SchemaRegistryClient    = schemaRegistryClient;
//         SchemaValidator = new JsonSchemaValidator();
//         SchemaGeneratorSettings = schemaGeneratorSettings;
//
//         Schema         = JsonSchema.FromType<T>(SchemaGeneratorSettings);
//         SchemaFullname = Schema.Title;
//         SchemaText     = Schema.ToJson();
//
//         // var nonJsonConfig = config.Where(item => !item.Key.StartsWith("json."));
//         // if (nonJsonConfig.Any()) throw new ArgumentException($"JsonSerializer: unknown configuration parameter {nonJsonConfig.First().Key}");
//
//         //if (config.BufferBytes != null) _initialBufferSize          = config.BufferBytes.Value;
//         //if (config.AutoRegisterSchemas != null) _autoRegisterSchema = config.AutoRegisterSchemas.Value;
//         //if (config.NormalizeSchemas != null) _normalizeSchemas      = config.NormalizeSchemas.Value;
//         //if (config.UseLatestVersion != null) _useLatestVersion      = config.UseLatestVersion.Value;
//         //if (config.LatestCompatibilityStrict != null) _latestCompatibilityStrict = config.LatestCompatibilityStrict.Value;
//         //if (config.SubjectNameStrategy != null) _subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate();
//
//         //if (_useLatestVersion && _autoRegisterSchema) throw new ArgumentException($"JsonSerializer: cannot enable both use.latest.version and auto.register.schemas");
//     }
//
//     ISchemaRegistryClient       SchemaRegistryClient    { get; }
//     JsonSchemaValidator         SchemaValidator         { get; }
//     JsonSchemaGeneratorSettings SchemaGeneratorSettings { get; }
//     JsonSchema                  Schema                  { get; }
//     string                      SchemaText              { get; }
//     string                      SchemaFullname          { get; }
//     bool                        AutoRegisterSchema      { get; }
//     int                         SchemaId                { get; set; }
//
//     /// <summary>
//     ///     Serialize an instance of type <typeparamref name="T"/> to a UTF8 encoded JSON
//     ///     represenation. The serialized data is preceeded by:
//     ///       1. A "magic byte" (1 byte) that identifies this as a message with
//     ///          Confluent Platform framing.
//     ///       2. The id of the schema as registered in Confluent's Schema Registry
//     ///          (4 bytes, network byte order).
//     ///     This call may block or throw on first use for a particular topic during
//     ///     schema registration / verification.
//     /// </summary>
//     /// <param name="value">
//     ///     The value to serialize.
//     /// </param>
//     /// <param name="context">
//     ///     Context relevant to the serialize operation.
//     /// </param>
//     /// <returns>
//     ///     A <see cref="System.Threading.Tasks.Task" /> that completes with
//     ///     <paramref name="value" /> serialized as a byte array.
//     /// </returns>
//     public async Task<byte[]> SerializeAsync(T value, SerializationContext context) {
//         if (value is null) return null;
//
//         // the json serializer will be chosen based on the configuration of the generator
//         var serializedString = SchemaGeneratorSettings.SerializerOptions is not null
//             ? JsonSerializer.Serialize(value, (JsonSerializerOptions) SchemaGeneratorSettings.SerializerOptions)
//             : Newtonsoft.Json.JsonConvert.SerializeObject(value, SchemaGeneratorSettings.ActualSerializerSettings);
//
//         var validationResult = SchemaValidator.Validate(serializedString, Schema);
//         if (validationResult.Count > 0)
//             throw new InvalidDataException($"Schema validation failed for properties: [{string.Join(", ", validationResult.Select(r => r.Path))}]");
//
//         try {
//             await SerializeMutex.WaitAsync().ConfigureAwait(false);
//
//             try {
//                 // var subject = _subjectNameStrategy != null
//                 //     // use the subject name strategy specified in the serializer config if available.
//                 //     ? _subjectNameStrategy(context, SchemaFullname)
//                 //     // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
//                 //     : context.Component == MessageComponentType.Key
//                 //         ? SchemaRegistryClient.ConstructKeySubjectName(context.Topic, SchemaFullname)
//                 //         : SchemaRegistryClient.ConstructValueSubjectName(context.Topic, SchemaFullname);
//
//                 var subject = GetSubjectNameAsRecord(context, SchemaFullname);
//
//                 if (!SubjectsRegistered.Contains(subject)) {
//                     SchemaId =  AutoRegisterSchema switch {
//                         true  =>  await SchemaRegistryClient.RegisterSchemaAsync(subject, new(SchemaText, EmptyReferencesList, SchemaType.Json)).ConfigureAwait(false),
//                         false => await SchemaRegistryClient.GetSchemaIdAsync(subject, new(SchemaText, EmptyReferencesList, SchemaType.Json)).ConfigureAwait(false)
//                     };
//
//                     SubjectsRegistered.Add(subject);
//                 }
//             }
//             finally {
//                 SerializeMutex.Release();
//             }
//
//             var manager = new RecyclableMemoryStreamManager();
//
//             using (var stream = manager.GetStream())
//             using (var writer = new BinaryWriter(stream)) {
//                 stream.WriteByte(Constants.MagicByte);
//                 writer.Write(IPAddress.HostToNetworkOrder(SchemaId));
//                 writer.Write(System.Text.Encoding.UTF8.GetBytes(serializedString));
//                 return stream.ToArray();
//             }
//         }
//         catch (AggregateException aex) {
//             throw aex.InnerException!;
//         }
//     }
// }