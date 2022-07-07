using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Krimson.SchemaRegistry.Configuration;

namespace Krimson.Processors.Configuration;

[PublicAPI]
public record KrimsonProcessorOptions {
    public KrimsonProcessorOptions() {
        InputTopics           = Array.Empty<string>();
        Router                = new KrimsonProcessorRouter();
        Interceptors          = new InterceptorCollection();
        ConsumerConfiguration = DefaultConfigs.DefaultConsumerConfig;
        ProducerConfiguration = DefaultConfigs.DefaultProducerConfig;
        RegistryBuilder       = new KrimsonSchemaRegistryBuilder();
        RegistryFactory       = () => RegistryBuilder.Create();
    }

    public string[]                                          InputTopics           { get; init; }
    public TopicSpecification?                               OutputTopic           { get; init; }
    public KrimsonProcessorRouter                            Router                { get; init; }
    public InterceptorCollection                             Interceptors          { get; init; }
    public ConsumerConfig                                    ConsumerConfiguration { get; init; }
    public ProducerConfig                                    ProducerConfiguration { get; init; }
    public KrimsonSchemaRegistryBuilder                             RegistryBuilder       { get; init; }
    public Func<ISchemaRegistryClient>                       RegistryFactory       { get; init; }
    public Func<ISchemaRegistryClient, IDynamicDeserializer> DeserializerFactory   { get; init; }
    public Func<ISchemaRegistryClient, IDynamicSerializer>   SerializerFactory     { get; init; }
}