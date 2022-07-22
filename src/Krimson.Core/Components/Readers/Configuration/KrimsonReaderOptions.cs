using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.Processors;
using Krimson.Serializers;

namespace Krimson.Readers.Configuration;

[PublicAPI]
public record KrimsonReaderOptions {
    public KrimsonReaderOptions() {
        Router                = new KrimsonProcessorRouter();
        Interceptors          = new InterceptorCollection();
        ConsumerConfiguration = DefaultConfigs.DefaultConsumerConfig;
    }

    public KrimsonProcessorRouter     Router                { get; init; }
    public InterceptorCollection      Interceptors          { get; init; }
    public ConsumerConfig             ConsumerConfiguration { get; init; }
    public Func<IDynamicDeserializer> DeserializerFactory   { get; init; }
}