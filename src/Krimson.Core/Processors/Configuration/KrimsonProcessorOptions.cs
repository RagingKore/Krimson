using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Krimson.Processors.Configuration;

[PublicAPI]
public record KrimsonProcessorOptions {
    public KrimsonProcessorOptions() {
        InputTopics           = Array.Empty<string>();
        Router                = new();
        Interceptors          = new();
        ConsumerConfiguration = DefaultConfigs.DefaultConsumerConfig;
        ProducerConfiguration = DefaultConfigs.DefaultProducerConfig;
        RegistryConfiguration = DefaultConfigs.DefaultSchemaRegistryConfig;
        RegistryFactory       = () => new CachedSchemaRegistryClient(RegistryConfiguration);
        // DeserializerFactory   = registry => new ProtobufDynamicDeserializer(registry);
        // SerializerFactory     = registry => new ProtobufDynamicSerializer(registry);
        LoggerFactory         = new NullLoggerFactory();
    }

    public string[]                                          InputTopics           { get; init; }
    public TopicSpecification?                               OutputTopic           { get; init; }
    public KrimsonProcessorRouter                            Router                { get; init; }
    public InterceptorCollection                             Interceptors          { get; init; }
    public ConsumerConfig                                    ConsumerConfiguration { get; init; }
    public ProducerConfig                                    ProducerConfiguration { get; init; }
    public SchemaRegistryConfig                              RegistryConfiguration { get; init; }
    public Func<ISchemaRegistryClient>                       RegistryFactory       { get; init; }
    public Func<ISchemaRegistryClient, IDynamicDeserializer> DeserializerFactory   { get; init; }
    public Func<ISchemaRegistryClient, IDynamicSerializer>   SerializerFactory     { get; init; }
    public ILoggerFactory                                    LoggerFactory         { get; init; }
}