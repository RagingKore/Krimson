using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Interceptors;
using Krimson.Processors;
using Krimson.SchemaRegistry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Krimson.Readers.Configuration;

[PublicAPI]
public record KrimsonReaderOptions {
    public KrimsonReaderOptions() {
        Router                = new KrimsonProcessorRouter();
        Interceptors          = new InterceptorCollection();
        ConsumerConfiguration = DefaultConfigs.DefaultConsumerConfig;
        RegistryConfiguration = DefaultConfigs.DefaultSchemaRegistryConfig;
        RegistryFactory       = () => new CachedSchemaRegistryClient(RegistryConfiguration);
        // DeserializerFactory   = registry => new ProtobufDynamicDeserializer(registry);
        LoggerFactory         = new NullLoggerFactory();
    }

    public KrimsonProcessorRouter                            Router                { get; init; }
    public InterceptorCollection                             Interceptors          { get; init; }
    public ConsumerConfig                                    ConsumerConfiguration { get; init; }
    public SchemaRegistryConfig                              RegistryConfiguration { get; init; }
    public Func<ISchemaRegistryClient>                       RegistryFactory       { get; init; }
    public Func<ISchemaRegistryClient, IDynamicDeserializer> DeserializerFactory   { get; init; }
    public ILoggerFactory                                    LoggerFactory         { get; init; }
}