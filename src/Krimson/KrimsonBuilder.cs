// ReSharper disable CheckNamespace

using Krimson.Processors.Configuration;
using Krimson.Processors.Hosting;
using Krimson.Producers;
using Krimson.Producers.Hosting;
using Krimson.SchemaRegistry.Configuration;
using Krimson.SchemaRegistry.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson.Extensions.DependencyInjection;

[PublicAPI]
public class KrimsonBuilder {
    public KrimsonBuilder(IServiceCollection services) => Services = services;

    IServiceCollection Services { get; }
    
    public KrimsonBuilder SchemaRegistry(Func<IConfiguration, IServiceProvider, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) {
        Services.AddKrimsonSchemaRegistry(build);
        return this;
    }

    public KrimsonBuilder SchemaRegistry(Func<IConfiguration, KrimsonSchemaRegistryBuilder, KrimsonSchemaRegistryBuilder> build) {
        Services.AddKrimsonSchemaRegistry(build);
        return this;
    }
    
        
    public KrimsonBuilder SchemaRegistry(string url, string apiKey = "", string apiSecret = "") { 
        Services.AddKrimsonSchemaRegistry((configuration, builder) => builder.Connection(url, apiKey, apiSecret));
        return this;
    }
    
    public KrimsonBuilder SchemaRegistry() {
        Services.AddKrimsonSchemaRegistry();
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
    
    public KrimsonBuilder Producer(Func<IConfiguration, IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
         Services.AddKrimsonProducer(build);
         return this;
    }

    public KrimsonBuilder Producer(Func<IConfiguration, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }
}