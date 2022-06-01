using Krimson.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using static System.String;
using static Krimson.DefaultConfigs;

namespace Krimson.Configuration;

[PublicAPI]
public static class ProducerBuilderExtensions {
    public static KrimsonProducerBuilder ReadSettings(this KrimsonProducerBuilder builder, IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));
        
        return builder
            .Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", builder.Options.ProducerConfiguration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", builder.Options.ProducerConfiguration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", builder.Options.ProducerConfiguration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", builder.Options.ProducerConfiguration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", builder.Options.ProducerConfiguration.SaslMechanism!.Value)
            )
            .SchemaRegistry(
                configuration.GetValue("Krimson:SchemaRegistry:Url", DefaultSchemaRegistryConfig.Url),
                configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
                configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            )
            .ClientId(
                configuration.GetValue(
                    "Krimson:Producer:ClientId",
                    configuration.GetValue("Krimson:ClientId", builder.Options.ProducerConfiguration.ClientId)
                )
            )
            //.Topic(configuration.GetValue("Krimson:Producer:Topic", ""))
            .With(bldr => {
                var topic = configuration.GetValue("Krimson:Producer:Topic", "");
                return !IsNullOrWhiteSpace(topic) ? bldr.Topic(topic) : bldr;
            });
    }
    
    // public static KrimsonProducerBuilder ReadSettings(this KrimsonProducerBuilder builder, IConfiguration configuration) {
    //     Ensure.NotNull(configuration, nameof(configuration));
    //
    //     var environment = configuration.GetValue<string?>("ASPNETCORE_ENVIRONMENT", null) ?? Environments.Development;
    //     
    //     var section = new ConfigurationBuilder()
    //         .AddConfiguration(configuration)
    //         .AddJsonFile("krimson.json", true)
    //         .AddJsonFile($"krimson.{environment}.json", true)
    //         .AddJsonFile($"krimson.{environment.ToLowerInvariant()}.json", true)
    //         .Build()
    //         .GetSection(nameof(Krimson));
    //
    //     return ReadSettings(builder, section);
    // }
    //
    // public static KrimsonProducerBuilder ReadSettings(this KrimsonProducerBuilder builder) {
    //     var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? Environments.Development;
    //
    //     var configuration = new ConfigurationBuilder()
    //         .AddJsonFile("appsettings.json", true)
    //         .AddJsonFile($"appsettings.{environment}.json", true)
    //         .AddJsonFile($"appsettings.{environment.ToLowerInvariant()}.json", true)
    //         .AddEnvironmentVariables()
    //         .Build();
    //
    //     return ReadSettings(builder, configuration);
    // }
}