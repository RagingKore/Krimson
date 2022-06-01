// ReSharper disable CheckNamespace

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Humanizer;
using Krimson;
using Krimson.Processors;
using Krimson.Producers;
using Nito.AsyncEx.Synchronous;
using Serilog;
using static System.String;
using static System.Threading.Tasks.Task;

await Run();

static async Task Run() {
    const int    processorTasks               = 16;
    const int    inputMessagesCount           = 1_056_000;
    const int    inputBatchSize               = 10;
    const int    processorMaxInFlightMessages = 10000;
    const string inputTopic                   = "krimson.detective.input";
    const int    inputTopicPartitions         = 16;
    const string outputTopic                  = "krimson.detective.output";
    const int    outputTopicPartitions        = 16;
    const int    timeout                      = 120_000;
    const int    startTaskDelay               = 3000;
    const int    runs                         = 1;

    var logger = Log.ForContext("SourceContext", "Krimson.Detective");

    /*
     * Create client
     */

    // app.Configuration
    var connection = new ClientConnection {
        BootstrapServers = null,
        Username         = null,
        Password         = null,
        SecurityProtocol = SecurityProtocol.Plaintext,
        SaslMechanism    = SaslMechanism.Gssapi
    };

    for (var run = 1; run <= runs; run++)
        await ExecuteTest(
            run, connection, inputTopic,
            inputTopicPartitions,
            outputTopic, outputTopicPartitions,
            inputMessagesCount, inputBatchSize,
            processorTasks, processorMaxInFlightMessages,
            timeout, startTaskDelay, logger
        );

    logger.Information("*** {Runs} test run(s) completed ***", runs);

}

static async Task ExecuteTest(
    int run,
    ClientConnection connection,
    string inputTopic,
    int inputTopicPartitions,
    string outputTopic,
    int outputTopicPartitions,
    int inputMessagesCount,
    int inputBatchSize,
    int processorTasks,
    int processorMaxInFlightMessages,
    int timeout,
    int startTaskDelay,
    ILogger logger
) {
    var subscription = $"krimson-{DateTime.UtcNow.Millisecond}";

    var adminClient = new AdminClientBuilder(DefaultConfigs.DefaultClientConfig).Build();
    
    /*
     * Delete topics
     */
    await adminClient.DeleteTopics(new() {
        inputTopic,
        outputTopic
    });
    
    await Delay(2000);
    logger.Information("*** test run {Run:00} | topics deleted ***", run);

    /*
     * Create topics
     */
    await adminClient.CreateTopic(inputTopic, inputTopicPartitions, 1);
    await adminClient.CreateTopic(outputTopic, outputTopicPartitions, 1);
    await Delay(1000);
    logger.Information("*** test run {Run:00} | topics created ***", run);

    /*
     * Generate messages
     */
    var generatedMessages = GenerateMessages(connection, inputTopic, inputMessagesCount, inputBatchSize, logger);

    /*
    * Process messages
    */
    var cancellator = new CancellationTokenSource(timeout);

    var tasks = new List<Task<List<KafkaRecord>>>();

    for (var id = 1; id <= processorTasks; id++) {
        if (id > 1) {
            logger.Information("*** test run {Run:00} | adding new consumer to group in {Delay}ms ***", run, startTaskDelay);
            await Delay(startTaskDelay);
        }

        var task = ProcessMessages(
            connection: connection,
            processorId: id,
            subscriptionName: subscription,
            inputTopic: inputTopic,
            outputTopic: outputTopic,
            maxInFlightMessages: processorMaxInFlightMessages,
            cancellator: cancellator,
            logger: logger
        );

        tasks.Add(task);
    }

    var results    = await WhenAll(tasks);
    var replicated = results.SelectMany(x => x).ToList();

    /*
     * Check extra/missing messages
     */
    if (replicated.Count > inputMessagesCount)
        logger.Fatal(
            "*** test run {Run:00} | processed {ReplicatedMessageCount} ({DuplicatesCount} extra) ***",
            run, replicated.Count, replicated.Count - inputMessagesCount
        );
    else if (replicated.Count < inputMessagesCount)
        logger.Fatal(
            "*** test run {Run:00} | processed {ReplicatedMessageCount} ({DuplicatesCount} missing) ***",
            run, replicated.Count, inputMessagesCount - replicated.Count
        );
    else
        logger.Information("*** test run {Run:00} | all {InputMessagesCount} message(s) processed ***", run, inputMessagesCount);

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
        logger.Fatal("*** test run {Run:00} | {DupsCount} duplicates found ***", run, duplicates.Length);
    else
        logger.Information("*** test run {Run:00} | no dupplicates found ***", run);

    var messages = await ProcessMessages(
        connection: connection,
        processorId: 0,
        subscriptionName: subscription,
        inputTopic: inputTopic,
        outputTopic: outputTopic,
        maxInFlightMessages: processorMaxInFlightMessages,
        cancellator: new CancellationTokenSource(10000),
        logger: logger
    );

    if (messages.Any())
        logger.Fatal("*** test run {Run:00} | sequences not acknowledged. {MessageCount} messages consumed ***", run, messages.Count);
    else
        logger.Information("*** test run {Run:00} | all sequences acknowledged ***", run);

    var consumedMessages = await ReadMessages(connection, outputTopic);

    if (consumedMessages.Count != inputMessagesCount)
        logger.Fatal("*** test run {Run:00} | Read only {MessageCount} messages ***", run, consumedMessages.Count);

    var messagesByPartition = consumedMessages
        .GroupBy(x => x.Id.Position.TopicPartition)
        .OrderBy(x => x.Key)
        .ToDictionary(x => x.Key, x => x.Select(z => (z.Id, (InputMessage) z.Value!)));

    foreach (var entry in messagesByPartition) {
        FluentAssertions.AssertionExtensions.Should(entry.Value).BeInAscendingOrder(x => x.Id.Position);

        //Check.That(entry.Value).IsInAscendingOrder();
    }
}

static async Task<Dictionary<TopicPartition, Dictionary<TopicPartitionOffset, InputMessage>>> GenerateMessages(
    ClientConnection connection,
    string inputTopic,
    int messageCount,
    int batchSize,
    ILogger logger
) {
    var lorem = new Bogus.DataSets.Lorem();

    await using var producer = KafkaProducer.Builder
        .Connection(connection)
        .Topic(inputTopic)
        //.EnableLogging()
        .Create();

    var generatedMessages = new ConcurrentQueue<(TopicPartitionOffset Sequence, InputMessage Message)>();

    var start = MicroProfiler.GetTimestamp();

    var batchId    = Guid.NewGuid();
    var batchOrder = 1;

    for (var i = 0; i < messageCount; i++) {
        var inputMessage = new InputMessage(i, batchId, batchOrder, lorem.Text());

        var producerMessage = ProducerRequest.Builder
            .Message(inputMessage)
            .Key(inputMessage.BatchId)
            .Create();

        producer.Produce(
            producerMessage, result => {
                if (result.DeliveryFailed) {
                    logger.Error(
                        result.Exception, "{GlobalOrder} message delivery failed: {ErrorMessage}",
                        inputMessage.GlobalOrder, result.Exception!.Message
                    );

                    throw result.Exception;
                }

                generatedMessages.Enqueue((result.RecordId, inputMessage));

                logger.Verbose(
                    "{GlobalOrder} message delivered: {MessageId}",
                    inputMessage.GlobalOrder, result.RecordId.ToString()
                );
            }
        );

        if (batchOrder != batchSize)
            batchOrder += 1;
        else {
            batchOrder = 1;
            batchId    = Guid.NewGuid();
        }
    }

    await producer.Flush();

    logger.Warning(
        "*** generated {MessageCount} in {ElapsedHumanReadable} ***",
        generatedMessages.Count, MicroProfiler.GetElapsed(start).Humanize(3)
    );

    return generatedMessages
        .GroupBy(x => x.Sequence.TopicPartition)
        .OrderBy(x => x.Key.Partition)
        .ToDictionary(x => x.Key, x => x.ToDictionary(k => k.Sequence, k => k.Message));
}

static async Task<List<KafkaRecord>> ProcessMessages(
    ClientConnection connection,
    int processorId,
    string? subscriptionName,
    string inputTopic,
    string outputTopic,
    int maxInFlightMessages,
    CancellationTokenSource cancellator,
    ILogger logger
) {
    var processedMessages = new ConcurrentBag<KafkaRecord>();

    var processor = KafkaProcessor.Builder
        .ProcessorName($"krimson-detective-{processorId:00}")
        .SubscriptionName(subscriptionName ?? "krimson-detective")
        .Connection(connection)
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
        .Create();

    await processor.RunUntilCompletion(cancellator.Token);

    var messageIds = processedMessages.Any()
        ? processedMessages.Select(x => x.Id.ToString()).ToList()
        : new();

    var duplicates = messageIds
        .GroupBy(x => x)
        .Where(g => g.Count() > 1)
        .Select(g => g.Key)
        .ToList();

    if (duplicates.Count > 0)
        logger.Fatal("[{ProcessorId}] finished: {DupesCount} duplicates found. sample: {Dupes}", processorId, duplicates.Count, duplicates.Take(3));
    else
        logger.Information("[{ProcessorId}] finished: no duplicates found", processorId);

    return processedMessages.ToList();
}

static async Task<List<KafkaRecord>> ReadMessages(ClientConnection connection, string topic, int? processorId = null) {
    var records     = new List<KafkaRecord>();
    var cancellator = new CancellationTokenSource(60000);

    var processor = KafkaProcessor.Builder
        .Connection(connection)
        .ProcessorName($"krimson-detective-reader-{processorId ?? DateTimeOffset.Now.ToUnixTimeSeconds()}")
        .InputTopic(topic)
        .Process<InputMessage>((message, ctx) => records.Add(ctx.Record))
        .Create();

    await processor.Start(cancellator.Token);

    return records;
}

public class InputMessage
{
    public InputMessage() { }

    public InputMessage(int globalOrder, Guid batchId, int batchOrder, string text) {
        GlobalOrder = globalOrder;
        BatchId     = batchId;
        BatchOrder  = batchOrder;
        Text        = text;
    }

    public string Text        { get; set; } = Empty;
    public int    GlobalOrder { get; set; }
    public Guid   BatchId     { get; set; }
    public int    BatchOrder  { get; set; }

    public override string ToString() => $"{GlobalOrder:0000000}-{BatchId.ToString("N").Substring(28, 4)}:{BatchOrder:000}";
}