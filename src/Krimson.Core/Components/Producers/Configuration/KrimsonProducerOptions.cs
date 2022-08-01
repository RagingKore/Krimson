using Confluent.Kafka;
using Krimson.Interceptors;
using Krimson.Serializers;

namespace Krimson.Producers;

[PublicAPI]
public record KrimsonProducerOptions {
    public KrimsonProducerOptions() {
        Configuration     = DefaultConfigs.DefaultProducerConfig;
        Interceptors      = new InterceptorCollection();
        SerializerFactory = null!;
    }

    public ProducerConfig           Configuration     { get; init; }
    public InterceptorCollection    Interceptors      { get; init; }
    public string?                  DefaultTopic      { get; init; }
    public Func<IDynamicSerializer> SerializerFactory { get; init; }
}