using System.Diagnostics;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Fixie;
using Krimson.OpenTelemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;
using static Serilog.Core.Constants;
using static Serilog.Log;

namespace Krimson.Persistence.MongoDB.Tests;

public class MongoDBTestContext : TestContext {
    static readonly ILogger Log = ForContext(SourceContextPropertyName, nameof(MongoDBTestContext));

    static MongoDBTestContext() {
        // Sdk.CreateTracerProviderBuilder()
        //     .AddSource(nameof(Krimson))
        //     .AddConsoleExporter();
    }

    public MongoDBTestContext() {
        Services = new ServiceCollection();

        ClientConnection = null!;
        AdminClient      = null!;
        SchemaRegistry   = null!;
        CreatedTopics    = new();

        TracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddKrimsonSource()
            .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName: "Krimson.Persistence.MongoDB.Tests"))
            .AddConsoleExporter()
            .AddInMemoryExporter(InMemoryActivities)
            .AddOtlpExporter(
                options => {
                    options.Protocol = OtlpExportProtocol.Grpc;
                })
            .Build()!;

        // TestListener = new ActivityListener {
        //     ShouldListenTo  = source => source.Name.StartsWith("Krimson"),
        //     Sample          = SampleAll,
        //     ActivityStopped = activity => Activities.Add(activity)
        // };
        //
        // ActivitySource.AddActivityListener(TestListener);
        //
        // static ActivitySamplingResult SampleAll(ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded;
    }

    public IServiceCollection Services { get; set; }


    public ClientConnection      ClientConnection { get; private set; }
    public IAdminClient          AdminClient      { get; private set; }
    public ISchemaRegistryClient SchemaRegistry   { get; private set; }
    public IConfiguration        Configuration    { get; private set; }

    List<string>   CreatedTopics      { get; }
    TracerProvider TracerProvider     { get; }
    List<Activity> InMemoryActivities { get; } = new();

    protected override async ValueTask SetUp() {
        Configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        Services.AddSingleton(Configuration);

        WithConnection(
            Configuration.GetValue("Krimson:Connection:BootstrapServers", DefaultConfigs.DefaultConsumerConfig.BootstrapServers)!,
            Configuration.GetValue("Krimson:Connection:Username", DefaultConfigs.DefaultConsumerConfig.SaslUsername)!,
            Configuration.GetValue("Krimson:Connection:Password", DefaultConfigs.DefaultConsumerConfig.SaslPassword)!,
            Configuration.GetValue("Krimson:Connection:SecurityProtocol", DefaultConfigs.DefaultConsumerConfig.SecurityProtocol!.Value),
            Configuration.GetValue("Krimson:Connection:SaslMechanism", DefaultConfigs.DefaultConsumerConfig.SaslMechanism!.Value)
        );

        WithSchemaRegistry(
            Configuration.GetValue("Krimson:SchemaRegistry:Url",  DefaultConfigs.DefaultSchemaRegistryConfig.Url)!,
            Configuration.GetValue("Krimson:SchemaRegistry:ApiKey", "")!,
            Configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")!
        );


        // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (AdminClient is null)
            AdminClient = new AdminClientBuilder(DefaultConfigs.DefaultClientConfig).Build();

        await AdminClient
            .DeleteTestTopics(topic => topic.StartsWith("tst.") || topic.EndsWith(".tst"))
            .ConfigureAwait(false);
    }

    protected override ValueTask CleanUp() {
        try {
            if (CreatedTopics.Any()) {
               // await AdminClient.DeleteTopics(CreatedTopics);
                Log.Information("deleted topics: {CreatedTopics}", CreatedTopics);
            }

            SchemaRegistry.Dispose();
            AdminClient.Dispose();
            TracerProvider.Dispose();
        }
        catch (Exception ex) {
            Log.Warning(ex, "disposed suddenly");
        }

        return ValueTask.CompletedTask;
    }

    public MongoDBTestContext WithConnection(
        string bootstrapServers, string username, string password,
        SecurityProtocol protocol = SecurityProtocol.Plaintext,
        SaslMechanism mechanism = SaslMechanism.Plain
    ) {
        ClientConnection = new() {
            BootstrapServers = bootstrapServers,
            Username         = username,
            Password         = password,
            SecurityProtocol = protocol,
            SaslMechanism    = mechanism
        };

        AdminClient = new AdminClientBuilder(
            DefaultConfigs.DefaultClientConfig.With(
                cfg => {
                    cfg.BootstrapServers = ClientConnection.BootstrapServers;
                    cfg.SaslUsername     = ClientConnection.Username;
                    cfg.SaslPassword     = ClientConnection.Password;
                    cfg.SecurityProtocol = ClientConnection.SecurityProtocol;
                    cfg.SaslMechanism    = ClientConnection.SaslMechanism;
                }
            )
        ).Build();

        return this;
    }

    public MongoDBTestContext WithSchemaRegistry(string url, string apiKey, string apiSecret) {
        SchemaRegistry = new CachedSchemaRegistryClient(
            DefaultConfigs.DefaultSchemaRegistryConfig.With(
                cfg => {
                    cfg.Url                        = url;
                    cfg.BasicAuthUserInfo          = $"{apiKey}:{apiSecret}";
                    cfg.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
                }
            )
        );

        return this;
    }

    #pragma warning disable CA1822
    public string GenerateUniqueClientName()                   => $"{Guid.NewGuid().ToString("N").Substring(32 - 5, 5)}-client".ToLowerInvariant();
    public string GetInputTopicName(string testProcessorName)  => $"{testProcessorName}.input.tst".ToLowerInvariant();
    public string GetOutputTopicName(string testProcessorName) => $"{testProcessorName}.output.tst".ToLowerInvariant();
    #pragma warning restore CA1822

    public async ValueTask<string> CreateTestTopic(string topic, int partitions) {
        var topicCreated = false;

        while (!topicCreated)
            try {
                topicCreated = await AdminClient.CreateTopic(topic, partitions, 1).ConfigureAwait(false);
            }
            catch (Exception) {
                topicCreated = await AdminClient.CreateTopic(topic, partitions, 3).ConfigureAwait(false);
            }

        CreatedTopics.Add(topic);

        Log.Information("test topic created: {TopicName}", topic);

        return topic;
    }
}