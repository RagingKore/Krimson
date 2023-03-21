using Krimson.Fixie;

namespace Krimson.Tests.Processors;

public class ProcessorConsumptionTests : TestFixture<KrimsonTestContext> {
    public ProcessorConsumptionTests(KrimsonTestContext context) : base(context) { }

    [TestCase(1)]
    [TestCase(3)]
    public async Task Consumes_Empty_Topic(int partitions) {
        // Arrange
        var clientId   = Context.GenerateUniqueProcessorName();
        var inputTopic = await Context.CreateInputTopic(clientId, partitions);

        // Act
        var result = await Context.ProcessMessages(clientId, inputTopic, 0, timeout: 3);

        // Assert
        result.AssertAllTopicsCaughtUp();
    }

    [TestCase(10, 1)]
    [TestCase(100, 3)]
    public async Task Consumes_Empty_Cleaned_Topic(int numberOfMessages, int partitions) {
        // Arrange
        var clientId   = Context.GenerateUniqueProcessorName();
        var inputTopic = await Context.CreateInputTopic(clientId, partitions);

        await Context.ProduceTestMessages(inputTopic, numberOfMessages);
        await Context.AdminClient.DeleteRecords(inputTopic, partitions);

        // Act
        var result = await Context.ProcessMessages(clientId, inputTopic, 0, timeout: 10);

        // Assert
        result.AssertAllTopicsCaughtUp();
    }

    [TestCase(10, 1)]
    [TestCase(100, 3)]
    public async Task Consumes_Empty_Cleaned_Twice_Topic(int numberOfMessages, int partitions) {
        // Arrange
        var clientId   = Context.GenerateUniqueProcessorName();
        var inputTopic = await Context.CreateInputTopic(clientId, partitions);

        await Context.ProduceTestMessages(inputTopic, numberOfMessages);
        await Context.AdminClient.DeleteRecords(inputTopic, partitions);
        await Context.ProduceTestMessages(inputTopic, numberOfMessages);
        await Context.AdminClient.DeleteRecords(inputTopic, partitions);

        // Act
        var result = await Context.ProcessMessages(clientId, inputTopic, 0, timeout: 10);

        // Assert
        result.AssertAllTopicsCaughtUp();
    }


    [TestCase(1)]
    [TestCase(500)]
    public async Task Consumes_Topic(int numberOfMessages) {
        // Arrange
        var clientId          = Context.GenerateUniqueProcessorName();
        var inputTopic        = await Context.CreateInputTopic(clientId, 1);
        var producedRecordIds = await Context.ProduceTestMessages(inputTopic, numberOfMessages);

        // Act
        var result = await Context.ProcessMessages(clientId, inputTopic, numberOfMessages, timeout: 120);

        // Assert
        result
            .AssertAllTopicsCaughtUp()
            .AssertAllMessagesProcessed(producedRecordIds);
    }

    [TestCase(1, 3)]
    [TestCase(3, 3)]
    [TestCase(60, 2)]
    [TestCase(80, 3)]
    public async Task Consumes_Partitioned_Topic(int numberOfMessages, int partitions) {
        // Arrange
        var clientId          = Context.GenerateUniqueProcessorName();
        var inputTopic        = await Context.CreateInputTopic(clientId, partitions);
        var producedRecordIds = await Context.ProduceTestMessages(inputTopic, numberOfMessages);

        // Act
        var result = await Context.ProcessMessages(clientId, inputTopic, numberOfMessages, timeout: 120);
        
        // Assert
        result
            .AssertAllTopicsCaughtUp()
            .AssertAllMessagesProcessed(producedRecordIds);
    }
}