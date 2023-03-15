using Krimson.Fixie;

namespace Krimson.Tests.Processors;

public class ProcessorStreamingTests : TestFixture<KrimsonTestContext> {
    public ProcessorStreamingTests(KrimsonTestContext context) : base(context) { }

    [TestCase(1, 1)]
    [TestCase(100, 3)]
    public async Task Consumes_Messages_And_Produces_Output(int numberOfMessages, int partitions) {
        // Arrange
        var clientId          = Context.GenerateUniqueProcessorName();
        var inputTopic        = await Context.CreateInputTopic(clientId, partitions);
        var outputTopic       = await Context.CreateOutputTopic(clientId, partitions);
        var producedRecordIds = await Context.ProduceTestMessages(inputTopic, numberOfMessages);

        // Act
        var streamResult = await Context.StreamMessages(clientId, inputTopic, outputTopic, numberOfMessages);

        // Assert
        KrimsonTestContext.Log.Information("asserting messages were processed and forwarded...");

        streamResult
            .AssertAllTopicsCaughtUp()
            .AssertAllMessagesProcessed(producedRecordIds);;

        KrimsonTestContext.Log.Information("asserting output was sent...");

        // ensuring output was produced by consuming the output topic

        var outputResult = await Context.ProcessMessages($"{clientId}.output-consumer", outputTopic, numberOfMessages);

        outputResult
            .AssertAllTopicsCaughtUp()
            .AssertAllMessagesProcessedByKeyAndValue(streamResult.ProcessedRecords);
    }
}