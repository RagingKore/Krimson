using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using FluentAssertions;
using JetBrains.Annotations;
using Serilog;
using Serilog.Formatting.Display;
using Serilog.Sinks.Xunit;
using Krimson.Processors;
using Krimson.Processors.Configuration;
using Krimson.Producers;
using Krimson.Tests.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Serilog.Events;
using Serilog.Exceptions;
using Xunit.Abstractions;
using Xunit.Sdk;

using static System.DateTimeOffset;
using static Google.Protobuf.WellKnownTypes.Timestamp;
using static System.Linq.Enumerable;
using static System.Reflection.BindingFlags;
using static System.TimeSpan;
using ILogger = Serilog.ILogger;

namespace Krimson.Tests.Processors;

[PublicAPI]
public sealed class KrimsonProcessorFixture : IDisposable {

    static KrimsonProcessorFixture() {
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "test");
        //
        // Serilog.Log.Logger = new LoggerConfiguration()
        //     .MinimumLevel.Debug()
        //     .MinimumLevel.Override("ConfluentProducerLogger", LogEventLevel.Information)
        //     .MinimumLevel.Override("ConfluentProcessorLogger", LogEventLevel.Information)
        //     .Enrich.FromLogContext()
        //     .Enrich.WithThreadId()
        //     .Enrich.WithExceptionDetails()
        //     .WriteTo.XunitOutput(XunitOutputSink = new())
        //     .CreateLogger();
    }

    static readonly MessageTemplateTextFormatter LogFormatter = new(
        "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext}{NewLine}{Message}{NewLine}{Exception}"
    );

    public KrimsonProcessorFixture() {
        CreatedTopics = new();

        Serilog.Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .MinimumLevel.Override("ConfluentProducerLogger", LogEventLevel.Information)
            .MinimumLevel.Override("ConfluentProcessorLogger", LogEventLevel.Information)
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .Enrich.WithExceptionDetails()
            .WriteTo.XunitOutput(XunitOutputSink = new())
            .CreateLogger();

        LoggerFactory = new NullLoggerFactory();
    }

    ClientConnection      ClientConnection { get; set; }
    IAdminClient          AdminClient      { get; set; }
    ISchemaRegistryClient SchemaRegistry   { get; set; }
    ILoggerFactory        LoggerFactory    { get; set; }
    
    List<string>          CreatedTopics    { get; }
    XunitOutputSinkProxy  XunitOutputSink  { get; }

    public ILogger Log { get; private set; } = null!;

    public void Dispose() {
        try {
            if (CreatedTopics.Any()) {
                AdminClient.DeleteTopics(CreatedTopics).GetAwaiter().GetResult();
                Log.Information("Deleted topics: {CreatedTopics}", CreatedTopics);
            }

            SchemaRegistry.Dispose();
            AdminClient.Dispose();
        }
        catch (Exception ex) {
            Serilog.Log.Warning(ex, "disposed suddenly");
        }
        finally {
            Serilog.Log.CloseAndFlush();
        }
    }

    public KrimsonProcessorFixture WithOutput<T>(ITestOutputHelper output) {
        XunitOutputSink.RedirectLogToOutput(output, LogFormatter);

        var testName = AssertionExtensions
            .As<XunitTest>(AssertionExtensions.As<TestOutputHelper>(output)!.GetType().GetField("test", NonPublic | Instance)!.GetValue(output)!)
            .DisplayName;

        Log           = Serilog.Log.ForContext("SourceContext", $"{typeof(T).Name}.{testName}");
        LoggerFactory = new LoggerFactory().AddSerilog(Log);
        
        return this;
    }

    public KrimsonProcessorFixture WithConnection(
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
                    cfg.BootstrapServers = bootstrapServers;
                    cfg.SaslUsername     = username;
                    cfg.SaslPassword     = password;
                    cfg.SecurityProtocol = protocol;
                    cfg.SaslMechanism    = mechanism;
                }
            )
        ).Build();

        return this;
    }

    public KrimsonProcessorFixture WithSchemaRegistry(string url, string apiKey, string apiSecret) {
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
            .EnableDebug()
            //.UseJson()
            .Create();

        var requests = Range(1, numberOfMessages).Select(CreateProducerMessage).ToArray();
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

        Log.Information(
            "sent {NumberOfMessages} test messages in: {Elapsed}",
            ids.Count, MicroProfiler.GetElapsedHumanReadable(start)
        );

        return ids.ToList();

        ProducerRequest CreateProducerMessage(int order) {
            var id = Guid.NewGuid();

            var msg = new KrimsonTestMessage {
                Id        = id.ToString(),
                Order     = order,
                Timestamp = FromDateTimeOffset(UtcNow)
            };

            return ProducerRequest.Builder
                .Message(msg)
                .Key(msg.Order)
                .RequestId(id)
                .Create();
        }
    }

    public async Task<(IReadOnlyCollection<KrimsonRecord> ProcessedRecords, IReadOnlyCollection<SubscriptionTopicGap> SubscriptionGap)> ProcessMessages(
        Func<KrimsonProcessorBuilder, KrimsonProcessorBuilder> buildProcessor, int numberOfMessages, bool produceOutput = false, int timeout = 120
    ) {
        var cancellator = new CancellationTokenSource(FromSeconds(timeout));
        var processed   = new List<KrimsonRecord>();

        void TestMessageHandler(KrimsonTestMessage msg, KrimsonProcessorContext ctx) {
            ctx.CancellationToken.ThrowIfCancellationRequested();

            processed.Add(ctx.Record);

            if (produceOutput) {
                ctx.Output(msg, ctx.Record.Key!);

                if (processed.Count == numberOfMessages)
                    cancellator.CancelAfter(FromSeconds(3));
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
                //.UseJson()
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

    public async Task DeleteTestTopics() {
        var existingTopics = AdminClient.GetMetadata(FromSeconds(60))
            .Topics.Where(x => x.Topic.StartsWith("consumes") || x.Topic.StartsWith("consumes") || x.Topic.StartsWith("tst.") || x.Topic.EndsWith(".tst"))
            .Select(x => x.Topic)
            .ToList();
    
        if (existingTopics.Any())
            await AdminClient.DeleteTopicsAsync(existingTopics).ConfigureAwait(false);
    }
}