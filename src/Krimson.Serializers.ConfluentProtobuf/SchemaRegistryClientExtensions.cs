using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Krimson.SchemaRegistry;

namespace Krimson.Serializers.ConfluentProtobuf;

[PublicAPI]
public static class SchemaRegistryClientExtensions {
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
        subjectName ??= GetSubjectNameAsRecord(new(), descriptor.FullName);

        if (RegisteredSubjects.TryGetValue(subjectName, out var messageSchema))
            return messageSchema;

        await RegisterMutex.WaitAsync().ConfigureAwait(false);

        try {
            var references = await client
                .GetSchemaReferences(descriptor.File)
                .ConfigureAwait(false);

            var schema = new Schema(
                descriptor.File.SerializedData.ToBase64(),
                references,
                SchemaType.Protobuf
            );

            var schemaId = await client
                .RegisterSchemaAsync(subjectName, schema)
                .ConfigureAwait(false);

            // required to actually get the version ffs...
            var registeredSchema = await client
                .GetLatestSchemaAsync(subjectName)
                .ConfigureAwait(false);

            return RegisteredSubjects[subjectName] = new MessageSchema(
                schemaId, subjectName, 
                descriptor.ClrType.FullName!,
                registeredSchema.Version
            );
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

    public static Task<RegisteredSchema> GetSchema(this ISchemaRegistryClient client, string subject, Schema schema, bool autoRegister = false) =>
        autoRegister
            ? RegisterSchema(client, subject, schema)
            : client.LookupSchemaAsync(subject, schema, true);

    public static async Task<List<SchemaReference>> GetSchemaReferences(
        this ISchemaRegistryClient client,
        FileDescriptor descriptor,
        SerializationContext context,
        bool autoRegister,
        ReferenceSubjectNameStrategyDelegate getSubjectName
    ) {
        var tasks = new Task<SchemaReference>[descriptor.Dependencies.Count];
    
        for (var i = 0; i < descriptor.Dependencies.Count; ++i)
            tasks[i] = GetSchemaReference(
                client, descriptor.Dependencies[i], context,
                autoRegister, getSubjectName
            );
    
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
    
    public static async Task<SchemaReference> GetSchemaReference(
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
    
        var registeredSchema = await client
            .GetSchema(subject, schema, autoRegister)
            .ConfigureAwait(false);
    
        return new SchemaReference(dependency.Name, subject, registeredSchema.Version);
    }
}