using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.SchemaRegistry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Krimson.Producers;

[PublicAPI]
public record KrimsonProducerOptions {
    public KrimsonProducerOptions() {
        ProducerConfiguration = DefaultConfigs.DefaultProducerConfig;
        Interceptors          = new();
        RegistryConfiguration = DefaultConfigs.DefaultSchemaRegistryConfig;
        RegistryFactory       = () => new CachedSchemaRegistryClient(RegistryConfiguration);
        // SerializerFactory     = registry => new ProtobufDynamicSerializer(registry);
        LoggerFactory         = new NullLoggerFactory();
    }

    public ProducerConfig                                  ProducerConfiguration { get; init; }
    public SchemaRegistryConfig                            RegistryConfiguration { get; init; }
    public InterceptorCollection                           Interceptors          { get; init; }
    public string?                                         DefaultTopic          { get; init; }
    public Func<ISchemaRegistryClient>                     RegistryFactory       { get; init; }
    public Func<ISchemaRegistryClient, IDynamicSerializer> SerializerFactory     { get; init; }
    public ILoggerFactory                                  LoggerFactory         { get; init; }
}