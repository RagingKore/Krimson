// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry;
using Krimson.Hosting;
using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.SchemaRegistry.Configuration;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Extensions.DependencyInjection;

[PublicAPI]
public class KrimsonBuilder {
    public KrimsonBuilder(IServiceCollection services) => Services = services;

    internal IServiceCollection Services { get; }
    
    public KrimsonBuilder SchemaRegistry(Func<IConfiguration, IServiceProvider, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) {
        Services.AddKrimsonSchemaRegistry(build);
        return this;
    }

    public KrimsonBuilder SchemaRegistry(Func<IConfiguration, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) {
        Services.AddKrimsonSchemaRegistry(build);
        return this;
    }
    
        
    public KrimsonBuilder SchemaRegistry(string url, string apiKey = "", string apiSecret = "") { 
        Services.AddKrimsonSchemaRegistry((_, builder) => builder.Connection(url, apiKey, apiSecret));
        return this;
    }
    
    public KrimsonBuilder SchemaRegistry() {
        Services.AddKrimsonSchemaRegistry();
        return this;
    }
    
    
    public KrimsonBuilder ProcessorModule<T>() where T : KrimsonProcessorModule, new() {
        Services.AddSingleton<KrimsonProcessorModule>(new T());
        return this;
    }
    
    public KrimsonBuilder ProcessorModule<T>(T module) where T : KrimsonProcessorModule, new() {
        Services.AddSingleton<KrimsonProcessorModule>(module);
        return this;
    }
    
    public KrimsonBuilder ProcessorModule<T>(Func<IServiceProvider, T> getModule) where T : KrimsonProcessorModule, new() {
        Services.AddSingleton<KrimsonProcessorModule>(getModule);
        return this;
    }
    
    public KrimsonBuilder Processor(
        int tasks,
        Func<IConfiguration, IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(tasks, build, initialize);
        return this;
    }
    
    public KrimsonBuilder Processor(
        Func<IConfiguration, IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(build, initialize);
        return this;
    }
    
    public KrimsonBuilder Processor(
        int tasks,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(tasks, build, initialize);
        return this;
    }
    
    public KrimsonBuilder Processor(
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(build, initialize);
        return this;
    }
    
    public KrimsonBuilder Processor(
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(build, initialize);
        return this;
    }
    
    public KrimsonBuilder Producer(Func<IConfiguration, IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
         Services.AddKrimsonProducer(build);
         return this;
    }

    public KrimsonBuilder Producer(Func<IConfiguration, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }
    
    public KrimsonBuilder Producer(Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }

    public KrimsonBuilder Producer(Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }

    public KrimsonBuilder Serializer(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        Services.AddSingleton(ctx => getSerializer(ctx.GetRequiredService<ISchemaRegistryClient>()));
        return this;
    }

    public KrimsonBuilder Deserializer(Func<ISchemaRegistryClient, IDynamicDeserializer> getDeserializer) {
        Services.AddSingleton(ctx => getDeserializer(ctx.GetRequiredService<ISchemaRegistryClient>()));
        return this;
    }
    
    public KrimsonBuilder SerializerFactory(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        Services.AddSingleton(getSerializer);
        return this;
    }

    public KrimsonBuilder DeserializerFactory(Func<ISchemaRegistryClient, IDynamicDeserializer> getDeserializer) {
        Services.AddSingleton(getDeserializer);
        return this;
    }
}