using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Krimson.Interceptors;
using Krimson.Serializers;

namespace Krimson.Processors.Configuration;

[PublicAPI]
public record KrimsonProcessorOptions {
    public KrimsonProcessorOptions() {
        InputTopics           = Array.Empty<string>();
        Router                = new KrimsonProcessorRouter();
        Interceptors          = new InterceptorCollection();
        ConsumerConfiguration = DefaultConfigs.DefaultConsumerConfig;
        ProducerConfiguration = DefaultConfigs.DefaultProducerConfig;
    }

    public string[]                   InputTopics           { get; init; }
    public TopicSpecification?        OutputTopic           { get; init; }
    public KrimsonProcessorRouter     Router                { get; init; }
    public InterceptorCollection      Interceptors          { get; init; }
    public ConsumerConfig             ConsumerConfiguration { get; init; }
    public ProducerConfig             ProducerConfiguration { get; init; }
    public Func<IDynamicDeserializer> DeserializerFactory   { get; init; }
    public Func<IDynamicSerializer>   SerializerFactory     { get; init; }
}