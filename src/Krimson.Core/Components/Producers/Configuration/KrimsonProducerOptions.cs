using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.SchemaRegistry.Configuration;
using Krimson.Serializers;

namespace Krimson.Producers;

[PublicAPI]
public record KrimsonProducerOptions {
    public KrimsonProducerOptions() {
        Configuration   = DefaultConfigs.DefaultProducerConfig;
        Interceptors    = new InterceptorCollection();
        RegistryBuilder = new KrimsonSchemaRegistryBuilder();
        RegistryFactory = () => RegistryBuilder.Create();
    }

    public ProducerConfig                                  Configuration     { get; init; }
    public KrimsonSchemaRegistryBuilder                    RegistryBuilder   { get; init; }
    public Func<ISchemaRegistryClient>                     RegistryFactory   { get; init; }
    public InterceptorCollection                           Interceptors      { get; init; }
    public string?                                         DefaultTopic      { get; init; }
    public Func<ISchemaRegistryClient, IDynamicSerializer> SerializerFactory { get; init; }
}