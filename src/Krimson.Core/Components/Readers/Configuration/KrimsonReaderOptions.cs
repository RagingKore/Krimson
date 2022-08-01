using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Serializers;

namespace Krimson.Readers.Configuration;

[PublicAPI]
public record KrimsonReaderOptions {
    public KrimsonReaderOptions() {
        Interceptors          = new InterceptorCollection();
        ConsumerConfiguration = DefaultConfigs.DefaultReaderConfig;
        DeserializerFactory   = null!;
    }
    
    public InterceptorCollection      Interceptors          { get; init; }
    public ConsumerConfig             ConsumerConfiguration { get; init; }
    public Func<IDynamicDeserializer> DeserializerFactory   { get; init; }
}