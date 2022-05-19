using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.SchemaRegistry.Protobuf;

namespace Krimson.Producers;

[PublicAPI]
public record KrimsonProducerOptions {
    public KrimsonProducerOptions() {
        Configuration     = DefaultConfigs.DefaultProducerConfig;
        Interceptors      = new();
        RegistryFactory   = () => new CachedSchemaRegistryClient(DefaultConfigs.DefaultSchemaRegistryConfig);
        SerializerFactory = registry => new ProtobufDynamicSerializer(registry);
    }

    public ProducerConfig                                  Configuration     { get; init; }
    public InterceptorCollection                           Interceptors      { get; init; }
    public string?                                         DefaultTopic      { get; init; }
    public Func<ISchemaRegistryClient>                     RegistryFactory   { get; init; }
    public Func<ISchemaRegistryClient, IDynamicSerializer> SerializerFactory { get; init; }
}