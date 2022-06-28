using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Krimson.Fixie;
using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.SchemaRegistry.Protobuf;
using Krimson.Tests.Messages;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog;

using static Google.Protobuf.WellKnownTypes.Timestamp;
using ILogger = Serilog.ILogger;

namespace Krimson.Tests;

public class KrimsonTestContext : TestContext {
    static readonly ILogger Log = Serilog.Log.ForContext("SourceContext", nameof(KrimsonTestContext));
    
    // public KrimsonTestContext() {
    //     ClientConnection = new();
    //     AdminClient      = null!;
    //     SchemaRegistry   = new CachedSchemaRegistryClient(DefaultConfigs.DefaultSchemaRegistryConfig);
    //     CreatedTopics    = new();
    //     LoggerFactory    = new LoggerFactory().AddSerilog();
    // }
    //
    // ClientConnection      ClientConnection { get; set; }
    // IAdminClient          AdminClient      { get; set; }
    // ISchemaRegistryClient SchemaRegistry   { get; set; }
    // ILoggerFactory        LoggerFactory    { get; }
    // List<string>          CreatedTopics    { get; }

    public KrimsonTestContext() {
        ClientConnection = null!;
        AdminClient      = null!;
        SchemaRegistry   = null!;
        CreatedTopics    = new List<string>();
        LoggerFactory    = new LoggerFactory().AddSerilog();
    }

    ClientConnection      ClientConnection { get; set; }
    IAdminClient          AdminClient      { get; set; }
    ISchemaRegistryClient SchemaRegistry   { get; set; }
    ILoggerFactory        LoggerFactory    { get; }
    List<string>          CreatedTopics    { get; }

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

    protected override async ValueTask CleanUp() {
        try {
            if (CreatedTopics.Any()) {
                await AdminClient.DeleteTopics(CreatedTopics);
                Log.Information("deleted topics: {CreatedTopics}", CreatedTopics);
            }

            SchemaRegistry.Dispose();
            AdminClient.Dispose();
        }
        catch (Exception ex) {
            Log.Warning(ex, "disposed suddenly");
        }
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

    public string GenerateUniqueProcessorName()                => $"{Guid.NewGuid().ToString("N").Substring(32 - 5, 5)}-processor".ToLowerInvariant();
    public string GetInputTopicName(string testProcessorName)  => $"{testProcessorName}.input.tst".ToLowerInvariant();
    public string GetOutputTopicName(string testProcessorName) => $"{testProcessorName}.output.tst".ToLowerInvariant();
    
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
            .SchemaRegistry(SchemaRegistry)
            .ClientId(topic)
            .Topic(topic)
            .LoggerFactory(LoggerFactory)
            //.EnableDebug()
            .UseProtobuf()
            .Create();

        var requests = Enumerable.Range(1, numberOfMessages).Select(CreateProducerMessage).ToArray();
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

        ProducerRequest CreateProducerMessage(int order) {
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
                .SchemaRegistry(SchemaRegistry)
                .Process<KrimsonTestMessage>(TestMessageHandler)
                .LoggerFactory(LoggerFactory)
                .UseProtobuf()
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