using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Krimson.SchemaRegistry;
using Serilog;
using static Serilog.Core.Constants;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public static class SchemaRegistryClientExtensions {
    static readonly ILogger Log = Serilog.Log.ForContext(SourceContextPropertyName, "Krimson.SchemaRegistry");

    static readonly SemaphoreSlim                                 RegisterMutex      = new(1);
    static readonly ConcurrentDictionary<string, MessageSchema>   RegisteredSubjects = new();
    static readonly ConcurrentDictionary<Type, MessageDescriptor> DescriptorsCache   = new();
    
    static readonly SubjectNameStrategyDelegate          GetSubjectNameAsRecord  = SubjectNameStrategy.Record.ToDelegate();
    static readonly ReferenceSubjectNameStrategyDelegate GetReferenceSubjectName = ReferenceSubjectNameStrategy.ReferenceName.ToDelegate();

    public static string GetSubjectName(this ISchemaRegistryClient client, MessageDescriptor descriptor) =>
        GetSubjectNameAsRecord(new(), descriptor.FullName);

    public static string GetSubjectName<T>(this ISchemaRegistryClient client, T message) where T : IMessage<T> =>
        GetSubjectNameAsRecord(new(), message.Descriptor.FullName);

    public static async Task<MessageSchema> RegisterMessage(this ISchemaRegistryClient client, MessageDescriptor descriptor, string? subjectName = null) {
        subjectName ??= GetSubjectNameAsRecord(new SerializationContext(), descriptor.FullName);

        Log.Debug("registering proto message {SubjectName} schema and all references if any...", subjectName);

        if (RegisteredSubjects.TryGetValue(subjectName, out var messageSchema)) {
            Log.Debug("message {SubjectName} schema already registered", subjectName);
            return messageSchema;
        }
        
        await RegisterMutex.WaitAsync().ConfigureAwait(false);

        try {
            Log.Debug("finding and/or registering all message {SubjectName} schema references...", subjectName);

            var references = await client
                .GetSchemaReferences(descriptor.File, autoRegister: true)
                .ConfigureAwait(false);

            var schema = new Schema(
                descriptor.File.SerializedData.ToBase64(),
                references,
                SchemaType.Protobuf
            );

            Log.Debug("registering message {SubjectName} full schema...", subjectName);

            var schemaId = await client
                .RegisterSchemaAsync(subjectName, schema)
                .ConfigureAwait(false);

            Log.Debug("message {SubjectName} schema registered with id {SchemaId}", subjectName, schemaId);
            Log.Debug("obtaining message {SubjectName} schema version...", subjectName);

            // required to actually get the version ffs...
            var registeredSchema = await client
                .GetLatestSchemaAsync(subjectName)
                .ConfigureAwait(false);

            Log.Debug(
                "message {SubjectName} schema confirmed to be registered with id {SchemaId} and version {Version}", subjectName, schemaId,
                registeredSchema.Version
            );

            return RegisteredSubjects[subjectName] = new MessageSchema(
                schemaId, subjectName,
                descriptor.ClrType.FullName!,
                registeredSchema.Version
            );

        }
        catch (Exception ex) {
            throw new Exception($"Failed to register message {descriptor.FullName}", ex);
        }
        finally {
            RegisterMutex.Release();
        }
    }

    public static Task<MessageSchema> RegisterMessage(this ISchemaRegistryClient client, Type messageType, string? subjectName = null) =>
        RegisterMessage(client, DescriptorsCache.GetOrAdd(messageType, type => Activator.CreateInstance(type)!.As<IMessage>().Descriptor), subjectName);
    
    public static Task<MessageSchema> RegisterMessage<T>(this ISchemaRegistryClient client, string? subjectName = null) where T : IMessage<T>, new() =>
        RegisterMessage(client, DescriptorsCache.GetOrAdd(typeof(T), _ => new T().Descriptor), subjectName);

    public static async Task<RegisteredSchema> RegisterSchema(this ISchemaRegistryClient client, string subject, Schema schema) {
        _ = await client
            .RegisterSchemaAsync(subject, schema)
            .ConfigureAwait(false);

        return await client
            .GetLatestSchemaAsync(subject)
            .ConfigureAwait(false);
        //
        // return await client
        //     .LookupSchemaAsync(subject, schema, true)
        //     .ConfigureAwait(false);
    }

    // public static Task<RegisteredSchema> GetSchema(this ISchemaRegistryClient client, string subject, Schema schema, bool autoRegister = false) {
    //     return autoRegister
    //         ? RegisterSchema(client, subject, schema)
    //         : client.LookupSchemaAsync(subject, schema, true);
    // }

    public static async Task<List<SchemaReference>> GetSchemaReferences(
        this ISchemaRegistryClient client,
        FileDescriptor descriptor,
        SerializationContext context,
        bool autoRegister,
        ReferenceSubjectNameStrategyDelegate getSubjectName
    ) {
        Log.Debug("getting message descriptor {DescriptorName} dependencies",  descriptor.Name);

        var tasks = new Task<SchemaReference>[descriptor.Dependencies.Count];

        for (var i = 0; i < descriptor.Dependencies.Count; ++i) {
            Log.Debug(
                "identified message descriptor {DescriptorName} dependency: {DependencyDescriptorName}", 
                descriptor.Name, descriptor.Dependencies[i].Name
            );
            
            tasks[i] = GetSchemaDependencyReference(
                client, descriptor.Dependencies[i], context,
                autoRegister, getSubjectName
            );
        }

        return (await Task.WhenAll(tasks).ConfigureAwait(false)).ToList();
    }
    
    public static Task<List<SchemaReference>> GetSchemaReferences(
        this ISchemaRegistryClient client,
        FileDescriptor descriptor,
        SerializationContext? context = null,
        bool autoRegister = true
    ) =>
        GetSchemaReferences(
            client, descriptor, context ?? new SerializationContext(),
            autoRegister, GetReferenceSubjectName
        );
    
    public static async Task<SchemaReference> GetSchemaDependencyReference(
        this ISchemaRegistryClient client,
        FileDescriptor dependency,
        SerializationContext context,
        bool autoRegister,
        ReferenceSubjectNameStrategyDelegate getSubjectName
    ) {
        var references = await GetSchemaReferences(
            client, dependency, context,
            autoRegister, getSubjectName
        ).ConfigureAwait(false);
    
        var subject = getSubjectName(context, dependency.Name);
        var schema  = new Schema(dependency.SerializedData.ToBase64(), references, SchemaType.Protobuf);

        if (autoRegister)
            Log.Debug("registering dependency descriptor {DescriptorName} schema", dependency.Name);
        else
            Log.Debug("looking up the schema for dependency descriptor {DescriptorName}", dependency.Name);

        if (autoRegister) {
            var registeredSchema = await RegisterSchema(client, subject, schema).ConfigureAwait(false);
            return new SchemaReference(dependency.Name, subject, registeredSchema.Version);
        }
        else {
            var registeredSchema = await client.LookupSchemaAsync(subject, schema, true).ConfigureAwait(false);
            return new SchemaReference(dependency.Name, subject, registeredSchema.Version);
        }

        // var registeredSchema = await client
        //     .GetSchema(subject, schema, autoRegister)
        //     .ConfigureAwait(false);
        //
        // return new SchemaReference(dependency.Name, subject, registeredSchema.Version);
    }
}