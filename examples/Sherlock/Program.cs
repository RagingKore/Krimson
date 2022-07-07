// ReSharper disable CheckNamespace

using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using FluentAssertions;
using Humanizer;
using Krimson;
using Krimson.Examples.Messages.Telemetry;
using Krimson.Processors;
using Krimson.Producers;
using Krimson.SchemaRegistry.Configuration;
using Krimson.SchemaRegistry.Protobuf;
using Nito.AsyncEx.Synchronous;
using Serilog;
using Serilog.Exceptions;
using Serilog.Sinks.SystemConsole.Themes;
using static System.Threading.Tasks.Task;
using static Serilog.Core.Constants;

await Run();

static async Task Run() {
    const int    processorTasks               = 1;
    const int    inputMessagesCount           = 1;
    const int    inputBatchSize               = 100;
    const int    processorMaxInFlightMessages = 10000;
    const string inputTopic                   = "krimson.sherlock.input";
    const int    inputTopicPartitions         = 1;
    const string outputTopic                  = "krimson.sherlock.output";
    const int    outputTopicPartitions        = 1;
    const int    timeout                      = 120_000;
    const int    startTaskDelay               = 3000;
    const int    runs                         = 1;

    const string logOutputTemplate = "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext}{NewLine}{Message}{NewLine}{Exception}";

    Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Debug()
        //.MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
        .Enrich.WithProperty(SourceContextPropertyName, "Krimson.Sherlock")
        .Enrich.FromLogContext()
        .Enrich.WithThreadId()
        .Enrich.WithExceptionDetails()
        .WriteTo.Logger(
            logger => logger
                //.Filter.ByExcluding(Matching.FromSource(nameof(Fixie)))
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate, outputTemplate: logOutputTemplate, applyThemeToRedirectedOutput: true)
        )
        .CreateLogger();
    
    /*
     * Create client
     */

    // app.Configuration
    // var connection = new ClientConnection {
    //     BootstrapServers = null,
    //     Username         = null,
    //     Password         = null,
    //     SecurityProtocol = SecurityProtocol.Plaintext,
    //     SaslMechanism    = SaslMechanism.Gssapi
    // };
    var connection = new ClientConnection();

    var registryClient = new KrimsonSchemaRegistryBuilder().Create();

    for (var run = 1; run <= runs; run++)
        await ExecuteTest(
            run, connection, registryClient, inputTopic,
            inputTopicPartitions,
            outputTopic, outputTopicPartitions,
            inputMessagesCount, inputBatchSize,
            processorTasks, processorMaxInFlightMessages,
            timeout, startTaskDelay
        );

    Log.Information("*** {Runs} test run(s) completed ***", runs);
}

static async Task ExecuteTest(
    int run,
    ClientConnection connection,
    ISchemaRegistryClient registryClient,
    string inputTopic,
    int inputTopicPartitions,
    string outputTopic,
    int outputTopicPartitions,
    int inputMessagesCount,
    int inputBatchSize,
    int processorTasks,
    int processorMaxInFlightMessages,
    int timeout,
    int startTaskDelay
) {
    var subscription = $"krimson-{DateTime.UtcNow.Millisecond}";

    var adminClient = new AdminClientBuilder(DefaultConfigs.DefaultClientConfig).Build();

    /*
     * Delete topics
     */
    await adminClient.DeleteTopics(
        new List<string> {
            inputTopic,
            outputTopic
        }
    );

    await Delay(2000);
    Log.Information("*** test run {Run:00} | topics deleted ***", run);

    /*
     * Create topics
     */
    await adminClient.CreateTopic(inputTopic, inputTopicPartitions, 1);
    await adminClient.CreateTopic(outputTopic, outputTopicPartitions, 1);
    await Delay(1000);
    Log.Information("*** test run {Run:00} | topics created ***", run);

    /*
     * Generate messages
     */
    var generatedMessages = await GenerateMessages(
        connection, registryClient, inputTopic, inputMessagesCount,
        inputBatchSize
    ).ConfigureAwait(false);

    /*
    * Process messages
    */
    var cancellator = new CancellationTokenSource(timeout);

    var tasks = new List<Task<List<KrimsonRecord>>>();

    for (var id = 1; id <= processorTasks; id++) {
        if (id > 1) {
            Log.Information("*** test run {Run:00} | adding new consumer to group in {Delay}ms ***", run, startTaskDelay);

            await Delay(startTaskDelay);
        }

        var task = ProcessMessages(
            connection,
            registryClient,
            id,
            subscription,
            inputTopic,
            outputTopic,
            cancellator
        );

        tasks.Add(task);
    }

    var results    = await WhenAll(tasks).ConfigureAwait(false);
    var replicated = results.SelectMany(x => x).ToList();

    /*
     * Check extra/missing messages
     */
    if (replicated.Count > inputMessagesCount)
        Log.Fatal(
            "*** test run {Run:00} | processed {ReplicatedMessageCount} ({DuplicatesCount} extra) ***",
            run, replicated.Count, replicated.Count - inputMessagesCount
        );
    else if (replicated.Count < inputMessagesCount)
        Log.Fatal(
            "*** test run {Run:00} | processed {ReplicatedMessageCount} ({DuplicatesCount} missing) ***",
            run, replicated.Count, inputMessagesCount - replicated.Count
        );
    else
        Log.Information("*** test run {Run:00} | all {InputMessagesCount} message(s) processed ***", run, inputMessagesCount);

    /*
     * Check overall duplicates
     */
    var duplicates = replicated
        .GroupBy(x => x.Position)
        .Where(g => g.Count() > 1)
        .Select(g => g.Key)
        .ToArray();

    var dupesFound = duplicates.Length > 0;

    if (dupesFound)
        Log.Fatal("*** test run {Run:00} | {DupsCount} duplicates found ***", run, duplicates.Length);
    else
        Log.Information("*** test run {Run:00} | no dupplicates found ***", run);

    var messages = await ProcessMessages(
        connection,
        registryClient,
        0,
        subscription,
        inputTopic,
        outputTopic,
        new CancellationTokenSource(10000)
    ).ConfigureAwait(false);

    if (messages.Any())
        Log.Fatal(
            "*** test run {Run:00} | sequences not acknowledged. {MessageCount} messages consumed ***", run,
            messages.Count
        );
    else
        Log.Information("*** test run {Run:00} | all sequences acknowledged ***", run);

    var consumedMessages = await ReadMessages(connection, registryClient, outputTopic).ConfigureAwait(false);

    if (consumedMessages.Count != inputMessagesCount)
        Log.Fatal("*** test run {Run:00} | Read only {MessageCount} messages ***", run, consumedMessages.Count);

    var messagesByPartition = consumedMessages
        .GroupBy(x => x.Id.Position.TopicPartition)
        .OrderBy(x => x.Key)
        .ToDictionary(x => x.Key, x => x.Select(z => (z.Id, (InputMessage)z.Value!)));

    foreach (var entry in messagesByPartition)
        entry.Value.Should().BeInAscendingOrder(x => x.Id.Position);

    //Check.That(entry.Value).IsInAscendingOrder();
}

static async Task<Dictionary<TopicPartition, Dictionary<TopicPartitionOffset, InputMessage>>> GenerateMessages(
    ClientConnection connection,
    ISchemaRegistryClient registryClient,
    string inputTopic,
    int messageCount,
    int batchSize
) {
    var lorem = new Bogus.DataSets.Lorem();

    await using var producer = KrimsonProducer.Builder
        .Connection(connection)
        .SchemaRegistry(registryClient)
        .ClientId("krimson-sherlock-generator")
        .Topic(inputTopic)
        .UseProtobuf()
        .Create();

    var generatedMessages = new ConcurrentQueue<(TopicPartitionOffset Sequence, InputMessage Message)>();

    var start = MicroProfiler.GetTimestamp();

    var batchId    = Guid.NewGuid();
    var batchOrder = 1;

    for (var i = 0; i < messageCount; i++) {
        // var inputMessage = new InputMessage(
        //     i, batchId, batchOrder,
        //     lorem.Text()
        // );

        var inputMessage = new InputMessage {
            Text        = lorem.Text(),
            GlobalOrder = i,
            BatchId     = batchId.ToString(),
            BatchOrder  = batchOrder
        };
        
        var producerMessage = ProducerRequest.Builder
            .Message(inputMessage)
            .Key(inputMessage.BatchId)
            .Create();

        producer.Produce(
            producerMessage, result => {
                if (!result.Success) {
                    Log.Error(
                        result.Exception, "{GlobalOrder} message delivery failed: {ErrorMessage}",
                        inputMessage.GlobalOrder, result.Exception!.Message
                    );

                    throw result.Exception;
                }

                generatedMessages.Enqueue((result.RecordId, inputMessage));

                Log.Debug(
                    "{GlobalOrder} message delivered: {MessageId}",
                    inputMessage.GlobalOrder, result.RecordId.ToString()
                );
            }
        );

        if (batchOrder != batchSize) {
            batchOrder += 1;
        }
        else {
            batchOrder = 1;
            batchId    = Guid.NewGuid();
        }
    }

    producer.Flush();

    Log.Warning(
        "*** generated {MessageCount} in {ElapsedHumanReadable} ***",
        generatedMessages.Count, MicroProfiler.GetElapsed(start).Humanize(3)
    );

    return generatedMessages
        .GroupBy(x => x.Sequence.TopicPartition)
        .OrderBy(x => x.Key.Partition)
        .ToDictionary(x => x.Key, x => x.ToDictionary(k => k.Sequence, k => k.Message));
}

static async Task<List<KrimsonRecord>> ProcessMessages(
    ClientConnection connection,
    ISchemaRegistryClient registryClient,
    int processorId,
    string? subscriptionName,
    string inputTopic,
    string outputTopic,
    CancellationTokenSource cancellator
) {
    var processedMessages = new ConcurrentBag<KrimsonRecord>();
    
    var processor = KrimsonProcessor.Builder
        .ClientId($"krimson-sherlock-{processorId:00}")
        .GroupId(subscriptionName ?? "krimson-sherlock")
        .Connection(connection)
        .SchemaRegistry(registryClient)
        .InputTopic(inputTopic)
        .OutputTopic(outputTopic)
        .Process<InputMessage>(
            (message, ctx) => {
                ctx.Output(message, ctx.Record.Key);

                if (processedMessages.Count % 1000 == 0)
                    Delay(100).WaitAndUnwrapException(); // delay to simulate work

                processedMessages.Add(ctx.Record);

                //
                // logger.Debug(
                //     "{ProcessorId} InputId:{InputId} Global Order:{GlobalOrder} message processed in order at {ProcessingOrder}: {MessageId} ",
                //     processorId, ctx.Message.InputId, ctx.Message.GlobalOrder, ctx.ProcessingOrder, ctx.ConsumerMessage.MessageId
                // );

                return CompletedTask;
            }
        )
        .UseProtobuf()
        .Create();

    await processor.RunUntilCompletion(cancellator.Token).ConfigureAwait(false);

    var messageIds = processedMessages.Any()
        ? processedMessages.Select(x => x.Id.ToString()).ToList()
        : new List<string>();

    var duplicates = messageIds
        .GroupBy(x => x)
        .Where(g => g.Count() > 1)
        .Select(g => g.Key)
        .ToList();

    if (duplicates.Count > 0)
        Log.Fatal(
            "[{ProcessorId}] finished: {DupesCount} duplicates found. sample: {Dupes}", processorId, duplicates.Count,
            duplicates.Take(3)
        );
    else
        Log.Information("[{ProcessorId}] finished: no duplicates found", processorId);

    return processedMessages.ToList();
}

static async Task<List<KrimsonRecord>> ReadMessages(
    ClientConnection connection, ISchemaRegistryClient registryClient, string topic, int? processorId = null
) {
    var records     = new List<KrimsonRecord>();
    var cancellator = new CancellationTokenSource(60000);

    var processor = KrimsonProcessor.Builder
        .Connection(connection)
        .SchemaRegistry(registryClient)
        .ClientId($"krimson-sherlock-reader-{processorId ?? DateTimeOffset.Now.ToUnixTimeSeconds()}")
        .InputTopic(topic)
        .Process<InputMessage>((message, ctx) => records.Add(ctx.Record))
        .UseProtobuf()
        .Create();

    await processor.Activate(cancellator.Token).ConfigureAwait(false);

    return records;
}

// public class InputMessage {
//     public InputMessage() { }
//
//     public InputMessage(int globalOrder, Guid batchId, int batchOrder, string text) {
//         GlobalOrder = globalOrder;
//         BatchId     = batchId;
//         BatchOrder  = batchOrder;
//         Text        = text;
//     }
//
//     public string Text        { get; set; } = Empty;
//     public int    GlobalOrder { get; set; }
//     public Guid   BatchId     { get; set; }
//     public int    BatchOrder  { get; set; }
//
//     public override string ToString() => $"{GlobalOrder:0000000}-{BatchId.ToString("N").Substring(28, 4)}:{BatchOrder:000}";
// }