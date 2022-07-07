using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.Processors;
using Krimson.SchemaRegistry;
using Krimson.Serializers;

namespace Krimson.Readers.Configuration;

[PublicAPI]
public record KrimsonReaderOptions {
    public KrimsonReaderOptions() {
        Router                = new KrimsonProcessorRouter();
        Interceptors          = new InterceptorCollection();
        ConsumerConfiguration = DefaultConfigs.DefaultConsumerConfig;
        RegistryConfiguration = DefaultConfigs.DefaultSchemaRegistryConfig;
        RegistryFactory       = () => new CachedSchemaRegistryClient(RegistryConfiguration);
    }

    public KrimsonProcessorRouter                            Router                { get; init; }
    public InterceptorCollection                             Interceptors          { get; init; }
    public ConsumerConfig                                    ConsumerConfiguration { get; init; }
    public SchemaRegistryConfig                              RegistryConfiguration { get; init; }
    public Func<ISchemaRegistryClient>                       RegistryFactory       { get; init; }
    public Func<ISchemaRegistryClient, IDynamicDeserializer> DeserializerFactory   { get; init; }
}