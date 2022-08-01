using System.Collections.Concurrent;
using System.Diagnostics;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Fixie;
using Krimson.OpenTelemetry;
using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Serializers.ConfluentProtobuf;
using Krimson.Tests.Messages;
using Microsoft.Extensions.Configuration;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;
using static Google.Protobuf.WellKnownTypes.Timestamp;
using static Serilog.Core.Constants;
using static Serilog.Log;

namespace Krimson.Tests;

public class KrimsonTestContext : TestContext {
    static readonly ILogger Log = ForContext(SourceContextPropertyName, nameof(KrimsonTestContext));

    static KrimsonTestContext() {
        // Sdk.CreateTracerProviderBuilder()
        //     .AddSource(nameof(Krimson.Tests))
        //     .AddConsoleExporter();
    }

    public KrimsonTestContext() {
        ClientConnection = null!;
        AdminClient      = null!;
        SchemaRegistry   = null!;
        CreatedTopics    = new List<string>();

        TracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddSource("Krimson.Tests")
            .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName: "Krimson.Tests"))
            .AddConsoleExporter()
            .AddInMemoryExporter(InMemoryActivities)
            .AddOtlpExporter(
                options => {
                    options.Protocol = OtlpExportProtocol.Grpc;
                })
            .Build();

        // TestListener = new ActivityListener {
        //     ShouldListenTo  = source => source.Name.StartsWith("Krimson"),
        //     Sample          = SampleAll,
        //     ActivityStopped = activity => Activities.Add(activity)
        // };
        //
        // ActivitySource.AddActivityListener(TestListener);

        // static ActivitySamplingResult SampleAll(ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded;
    }

    public ClientConnection      ClientConnection   { get; private set; }
    public IAdminClient          AdminClient        { get; private set; }
    public ISchemaRegistryClient SchemaRegistry     { get; private set; }

    List<string>   CreatedTopics      { get; }
    TracerProvider TracerProvider     { get; }
    List<Activity> InMemoryActivities { get; } = new();

    protected override async ValueTask SetUp() {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        WithConnection(
            configuration.GetValue("Krimson:Connection:BootstrapServers", DefaultConfigs.DefaultConsumerConfig.BootstrapServers),
            configuration.GetValue("Krimson:Connection:Username", DefaultConfigs.DefaultConsumerConfig.SaslUsername),
            configuration.GetValue("Krimson:Connection:Password", DefaultConfigs.DefaultConsumerConfig.SaslPassword),
            configuration.GetValue("Krimson:Connection:SecurityProtocol", DefaultConfigs.DefaultConsumerConfig.SecurityProtocol!.Value),
            configuration.GetValue("Krimson:Connection:SaslMechanism", DefaultConfigs.DefaultConsumerConfig.SaslMechanism!.Value)
        );

        WithSchemaRegistry(
            configuration.GetValue("Krimson:SchemaRegistry:Url",  DefaultConfigs.DefaultSchemaRegistryConfig.Url),
            configuration.GetValue("Krimson:SchemaRegistry:ApiKey", ""),
            configuration.GetValue("Krimson:SchemaRegistry:ApiSecret", "")
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

    public KrimsonTestContext WithConnection(
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

    public KrimsonTestContext WithSchemaRegistry(string url, string apiKey, string apiSecret) {
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
    public string GenerateUniqueProcessorName()                => $"{Guid.NewGuid().ToString("N").Substring(32 - 5, 5)}-processor".ToLowerInvariant();
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

    public ValueTask<string> CreateInputTopic(string testProcessorName, int partitions) =>
        CreateTestTopic(GetInputTopicName(testProcessorName), partitions);

    public ValueTask<string> CreateOutputTopic(string testProcessorName, int partitions) =>
        CreateTestTopic(GetOutputTopicName(testProcessorName), partitions);

    public async Task<List<RecordId>> ProduceTestMessages(string topic, int numberOfMessages = 1000) {
        var start = MicroProfiler.GetTimestamp();
        
        await using var producer = KrimsonProducer.Builder
            .Connection(ClientConnection)
            .UseProtobuf(SchemaRegistry)
            .ClientId(topic)
            .Topic(topic)
            //.EnableDebug()
            .Intercept(new OpenTelemetryProducerInterceptor(nameof(Krimson.Tests)))
            .Create();

        var requests = Enumerable.Range(1, numberOfMessages).Select(CreateMessage).ToArray();
        var ids      = new ConcurrentBag<RecordId>();
        var failed   = false;
        
        foreach (var message in requests) {
            producer.Produce(message, result => {
                if (result.Success)
                    ids.Add(result.RecordId);
                else
                    failed = true;
            });
        }

        producer.Flush();

        if(failed || ids.Count != numberOfMessages) {
            throw new($"only sent {ids.Count}/{numberOfMessages} test messages in: {MicroProfiler.GetElapsedHumanReadable(start)}");
        }

        Log.Debug(
            "sent {NumberOfMessages} test messages in: {Elapsed}",
            ids.Count, MicroProfiler.GetElapsedHumanReadable(start)
        );

        return ids.ToList();

        ProducerRequest CreateMessage(int order) {
            var id = Guid.NewGuid();

            var msg = new KrimsonTestMessage {
                Id        = id.ToString(),
                Order     = order,
                Timestamp = FromDateTimeOffset(DateTimeOffset.UtcNow)
            };
            
            return ProducerRequest.Builder
                .Message(msg)
                .Key(msg.Order)
                .RequestId(id)
                .Create();
            //
            // return msg.ToProduceRequest(msg.Order, id);
            //
            // return msg
            //     .ProduceRequest()
            //     .Key(msg.Order)
            //     .RequestId(id)
            //     .Create();
        }
    }

    public async Task<(IReadOnlyCollection<KrimsonRecord> ProcessedRecords, IReadOnlyCollection<SubscriptionTopicGap> SubscriptionGap)> ProcessMessages(
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> buildProcessor, 
        int numberOfMessages, bool produceOutput = false, int timeout = 120
    ) {
        var cancellator = new CancellationTokenSource(TimeSpan.FromSeconds(timeout));
        var processed   = new List<KrimsonRecord>();

        void TestMessageHandler(KrimsonTestMessage msg, KrimsonProcessorContext ctx) {
            ctx.CancellationToken.ThrowIfCancellationRequested();

            processed.Add(ctx.Record);

            if (produceOutput) {
                ctx.Output(msg, ctx.Record.Key!);

                if (processed.Count == numberOfMessages)
                    cancellator.CancelAfter(TimeSpan.FromSeconds(3));
            }
            else if (processed.Count == numberOfMessages)
                cancellator.Cancel();
        }

        try {
            await using var processor = KrimsonProcessor.Builder
                .With(buildProcessor)
                .Connection(ClientConnection)
                .UseProtobuf(SchemaRegistry)
                .Process<KrimsonTestMessage>(TestMessageHandler)
                .Intercept(new OpenTelemetryProcessorInterceptor("Krimson.Tests"))
                .Create();

            var gap = await processor
                .RunUntilCompletion(cancellator.Token)
                .ConfigureAwait(false);

            return (processed, gap);
        }
        finally {
            cancellator.Dispose();
        }
    }

    public Task<(IReadOnlyCollection<KrimsonRecord> ProcessedRecords, IReadOnlyCollection<SubscriptionTopicGap> SubscriptionGap)> StreamMessages(
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> buildProcessor, int numberOfMessages, int timeout = 30
    ) => ProcessMessages(buildProcessor, numberOfMessages, true, timeout);
}