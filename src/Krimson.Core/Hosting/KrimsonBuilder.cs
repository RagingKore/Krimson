// ReSharper disable CheckNamespace

using Confluent.SchemaRegistry;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Readers.Configuration;
using Krimson.Serializers;
using Microsoft.Extensions.DependencyInjection;

namespace Krimson;

[PublicAPI]
public class KrimsonBuilder {
    public KrimsonBuilder(IServiceCollection services, string?  clientId) {
        Services = services;
        ClientId = clientId;
    }

    internal IServiceCollection Services { get; }
    internal string?            ClientId { get; }

    public KrimsonBuilder AddSchemaRegistry(string url, string apiKey = "", string apiSecret = "") {
        Services.AddKrimsonSchemaRegistry((_, builder) => builder.Connection(url, apiKey, apiSecret));
        return this;
    }

    public KrimsonBuilder AddSchemaRegistry() {
        Services.AddKrimsonSchemaRegistry();
        return this;
    }

    public KrimsonBuilder AddProcessor(
        int tasks,
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Services.AddKrimsonProcessor(
            tasks, 
            (ctx, builder) => build(ctx, ClientId is null ? builder : builder.ClientId(ClientId)),
            initialize
        );
        
        return this;
    }

    public KrimsonBuilder AddProcessor(
        int tasks,
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Services.AddKrimsonProcessor(tasks, builder => build(ClientId is null ? builder : builder.ClientId(ClientId)), initialize);
        return this;
    }

    public KrimsonBuilder AddProcessor(
        Func<IServiceProvider, KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Services.AddKrimsonProcessor((ctx, builder) => build(ctx, ClientId is null ? builder : builder.ClientId(ClientId)), initialize);
        return this;
    }

    public KrimsonBuilder AddProcessor(
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> build,
        Func<IServiceProvider, CancellationToken, Task>? initialize = null
    ) {
        Services.AddKrimsonProcessor(builder => build(ClientId is null ? builder : builder.ClientId(ClientId)), initialize);
        return this;
    }

    public KrimsonBuilder AddProducer(Func<IServiceProvider, KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }

    public KrimsonBuilder AddProducer(Func<KrimsonProducerBuilder, KrimsonProducerBuilder> build) {
        Services.AddKrimsonProducer(build);
        return this;
    }

    public KrimsonBuilder AddReader(Func<IServiceProvider, KrimsonReaderBuilder, KrimsonReaderBuilder> build) {
        Services.AddKrimsonReader(build);
        return this;
    }

    public KrimsonBuilder AddReader(Func<KrimsonReaderBuilder, KrimsonReaderBuilder> build) {
        Services.AddKrimsonReader((_, builder) => build(builder));
        return this;
    }

    public KrimsonBuilder AddReader() {
        Services.AddKrimsonReader();
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
}