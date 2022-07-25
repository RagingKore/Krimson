// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry;
using Krimson.Connectors;
using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Readers.Configuration;
using Krimson.SchemaRegistry.Configuration;
using Krimson.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public class KrimsonBuilder {
    public KrimsonBuilder(IServiceCollection services) => Services = services;

    internal IServiceCollection Services { get; }
    
    public KrimsonBuilder AddSchemaRegistry(Func<IConfiguration, IServiceProvider, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) {
        Services.AddKrimsonSchemaRegistry(build);
        return this;
    }

    public KrimsonBuilder AddSchemaRegistry(Func<IConfiguration, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) {
        Services.AddKrimsonSchemaRegistry(build);
        return this;
    }
    
        
    public KrimsonBuilder AddSchemaRegistry(string url, string apiKey = "", string apiSecret = "") { 
        Services.AddKrimsonSchemaRegistry((_, builder) => builder.Connection(url, apiKey, apiSecret));
        return this;
    }
    
    public KrimsonBuilder AddSchemaRegistry() {
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
    
    // public KrimsonBuilder AddProcessor(
    //     int tasks,
    //     Func<IConfiguration, IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
    //     Func<IServiceProvider, CancellationToken, Task>? initialize = null
    // ) { 
    //     Services.AddKrimsonProcessor(tasks, build, initialize);
    //     return this;
    // }
    //
    // public KrimsonBuilder AddProcessor(
    //     Func<IConfiguration, IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
    //     Func<IServiceProvider, CancellationToken, Task>? initialize = null
    // ) { 
    //     Services.AddKrimsonProcessor(build, initialize);
    //     return this;
    // }
    
    public KrimsonBuilder AddProcessor(
        int tasks,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(tasks, build, initialize);
        return this;
    }
    
    public KrimsonBuilder AddProcessor(
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(build, initialize);
        return this;
    }
    
    public KrimsonBuilder AddProcessor(
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) { 
        Services.AddKrimsonProcessor(build, initialize);
        return this;
    }
    
    // public KrimsonBuilder AddProducer(Func<IConfiguration, IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
    //      Services.AddKrimsonProducer(build);
    //      return this;
    // }
    //
    // public KrimsonBuilder AddProducer(Func<IConfiguration, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
    //     Services.AddKrimsonProducer(build);
    //     return this;
    // }
    
    public KrimsonBuilder AddProducer(Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }

    public KrimsonBuilder AddProducer(Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }
    
    // public KrimsonBuilder AddReader(Func<IConfiguration, IServiceProvider, KrimsonReaderBuilder, KrimsonReaderBuilder> build) {
    //     Services.AddKrimsonReader(build);
    //     return this;
    // }
    //
    // public KrimsonBuilder AddReader(Func<IConfiguration, KrimsonReaderBuilder, KrimsonReaderBuilder> build) {
    //     Services.AddKrimsonReader((cfg, ctx, builder) => build(cfg, builder));
    //     return this;
    // }
    
    public KrimsonBuilder AddReader(Func<IServiceProvider, KrimsonReaderBuilder, KrimsonReaderBuilder> build) {
        Services.AddKrimsonReader((cfg, ctx, builder) => build(ctx, builder));
        return this;
    }

    public KrimsonBuilder AddReader(Func<KrimsonReaderBuilder, KrimsonReaderBuilder> build) {
        Services.AddKrimsonReader((cfg, ctx, builder) => build(builder));
        return this;
    }

    public KrimsonBuilder AddSerializer(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        Services.AddSingleton(ctx => getSerializer(ctx.GetRequiredService<ISchemaRegistryClient>()));
        return this;
    }

    public KrimsonBuilder AddDeserializer(Func<ISchemaRegistryClient, IDynamicDeserializer> getDeserializer) {
        Services.AddSingleton(ctx => getDeserializer(ctx.GetRequiredService<ISchemaRegistryClient>()));
        return this;
    }
    
    public KrimsonBuilder AddSerializerFactory(Func<ISchemaRegistryClient, IDynamicSerializer> getSerializer) {
        Services.AddSingleton(getSerializer);
        return this;
    }

    public KrimsonBuilder AddDeserializerFactory(Func<ISchemaRegistryClient, IDynamicDeserializer> getDeserializer) {
        Services.AddSingleton(getDeserializer);
        return this;
    }
    
    public KrimsonBuilder AddPeriodicSourceConnector<T>(Action<PeriodicSourceConnectorOptions>? configure = null) where T : PullSourceConnector {
        Services.AddKrimsonPeriodicSourceConnector<T>(configure);
        return this;
    }
    
    
}