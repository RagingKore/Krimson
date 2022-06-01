using Krimson.Processors.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using static System.String;
using static Krimson.DefaultConfigs;

namespace Krimson.Configuration;

public static class ProcessorBuilderExtensions {
    public static KrimsonProcessorBuilder ReadSettings(this KrimsonProcessorBuilder builder, IConfiguration configuration) {
        Ensure.NotNull(configuration, nameof(configuration));

        return builder
            .Connection(
                configuration.GetValue("Krimson:Connection:BootstrapServers", builder.Options.ConsumerConfiguration.BootstrapServers),
                configuration.GetValue("Krimson:Connection:Username", builder.Options.ConsumerConfiguration.SaslUsername),
                configuration.GetValue("Krimson:Connection:Password", builder.Options.ConsumerConfiguration.SaslPassword),
                configuration.GetValue("Krimson:Connection:SecurityProtocol", builder.Options.ConsumerConfiguration.SecurityProtocol!.Value),
                configuration.GetValue("Krimson:Connection:SaslMechanism", builder.Options.ConsumerConfiguration.SaslMechanism!.Value)
            )
            .SchemaRegistry(
                configuration.GetValue("Krimson:SchemaRegistry:Url", DefaultSchemaRegistryConfig.Url),
                configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
                configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
            )
            .ClientId(
                configuration.GetValue(
                    "Krimson:Processor:ClientId",
                    configuration.GetValue("Krimson:ClientId", builder.Options.ConsumerConfiguration.ClientId)
                )
            )
            .GroupId(
                configuration.GetValue(
                    "Krimson:Processor:GroupId",
                    configuration.GetValue("Krimson:GroupId", builder.Options.ConsumerConfiguration.GroupId)
                )
            )
            .InputTopic(
                configuration
                    .GetValues("Krimson:Processor:InputTopic")
                    .Concat(configuration.GetValues("Krimson:Processor:Topic"))
                    .Distinct()
                    .ToArray()
            )
            .OutputTopic(
                configuration.GetValue(
                    "Krimson:Processor:OutputTopic",
                    configuration.GetValue("Krimson:Producer:Topic", "")
                )
            );
    }
    
    // public static KrimsonProcessorBuilder ReadSettings(this KrimsonProcessorBuilder builder, IConfiguration configuration) {
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
    // public static KrimsonProcessorBuilder ReadSettings(this KrimsonProcessorBuilder builder) {
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
 
   