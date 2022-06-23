using Confluent.Kafka;
using FluentAssertions;
using Krimson.Fixie;

namespace Krimson.Tests;

public class ProcessorStreamingTests : TestFixture<KrimsonTestContext> {
    public ProcessorStreamingTests(KrimsonTestContext context) : base(context) { }
    
    [TestCase(1, 1)]
    // [TestCase(10, 1)]
    // [TestCase(3, 3)]
    // [TestCase(4, 3)]
    // [TestCase(30, 8)]
    // [TestCase(50, 16)]
    // [TestCase(100, 8)]
    // [TestCase(300, 16)]
    public async Task Processes_Messages_And_Produces_Output(int numberOfMessages, int partitions) {
        // Arrange
        var processorName     = Context.GenerateUniqueProcessorName();
        var inputTopic        = await Context.CreateInputTopic(processorName, partitions);
        var outputTopic       = await Context.CreateOutputTopic(processorName, partitions);
        var producedRecordIds = await Context.ProduceTestMessages(inputTopic, numberOfMessages);

        // Act
        var (processedRecords, subscriptionGap) = await Context.StreamMessages(
            proc => proc
                .ClientId(processorName)
                .InputTopic(inputTopic)
                .OutputTopic(outputTopic),
            numberOfMessages
        );
        
        // Assert
        //Context.Log.Information("asserting messages were processed and forwarded...");
        
        processedRecords.Should().HaveSameCount(producedRecordIds);
        
        subscriptionGap.First(x => x.Topic == inputTopic).CaughtUp.Should().BeTrue();

       // Context.Log.Information("asserting output was sent...");
        
        var (processedOutputRecords, subscriptionOutputTopicGap) = await Context.ProcessMessages(
            proc => proc
                .ClientId($"{processorName}.output-consumer")
                .InputTopic(outputTopic),
            numberOfMessages
        );
        
        subscriptionOutputTopicGap.First(x => x.Topic == outputTopic).CaughtUp.Should().BeTrue();
        // processedOutputRecords.Select(x => (x.Key, x.Value))
        //     .Should().BeEquivalentTo(processedRecords.Select(x => (x.Key, x.Value)));
    }
}