using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Krimson.Interceptors;
using Krimson.Serializers;
using Krimson.State;

namespace Krimson.Processors.Configuration;

[PublicAPI]
public record KrimsonProcessorOptions {
    public KrimsonProcessorOptions() {
        InputTopics           = Array.Empty<string>();
        Interceptors          = new InterceptorCollection();
        ConsumerConfiguration = DefaultConfigs.DefaultConsumerConfig;
        ProducerConfiguration = DefaultConfigs.DefaultProducerConfig;
        Router                = new KrimsonMasterRouter();
        DeserializerFactory   = null!;
        SerializerFactory     = null!;
        StateStoreFactory     = () => new InMemoryStateStore();
    }

    public string[]                   InputTopics           { get; init; }
    public TopicSpecification?        OutputTopic           { get; init; }
    public InterceptorCollection      Interceptors          { get; init; }
    public ConsumerConfig             ConsumerConfiguration { get; init; }
    public ProducerConfig             ProducerConfiguration { get; init; }
    public Func<IDynamicDeserializer> DeserializerFactory   { get; init; }
    public Func<IDynamicSerializer>   SerializerFactory     { get; init; }
    public KrimsonMasterRouter        Router                { get; init; }
    public Func<IStateStore>          StateStoreFactory     { get; init; }
}