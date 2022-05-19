using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Krimson.Tests.Processors;

public class ProcessorTests : IClassFixture<KrimsonProcessorFixture> {
    public ProcessorTests(KrimsonProcessorFixture fixture, ITestOutputHelper output) {
        Fixture = fixture
            .WithOutput<ProcessorTests>(output);
    }

    KrimsonProcessorFixture Fixture { get; }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(10, 1)]
    [InlineData(3, 3)]
    [InlineData(4, 3)]
    [InlineData(30, 8)]
    // [InlineData(50, 16)]
    [InlineData(1000, 8)]
    // [InlineData(300, 16)]
    public async Task processes_messages(int numberOfMessages, int partitions) {
        //Arrange
        var processorName     = Fixture.GetUniqueProcessorName(nameof(processes_messages));
        var inputTopic        = await Fixture.CreateInputTopic(processorName, partitions);
        var producedRecordIds = await Fixture.ProduceTestMessages(inputTopic, numberOfMessages);
        
        //Act
        var (processedRecords, subscriptionGap) = await Fixture.ProcessMessages(
            proc => proc
                .ProcessorName(processorName)
                .InputTopic(inputTopic),
            numberOfMessages
        );
        
        //Assert
        Fixture.Log.Information("Asserting input was processed and forwarded...");

        processedRecords.Should().HaveSameCount(producedRecordIds);
        subscriptionGap.First(x => x.Topic == inputTopic).CaughtUp.Should().BeTrue();
        processedRecords.Select(x => x.Id).Should().BeEquivalentTo(producedRecordIds);
    }
    
    [Theory]
    [InlineData(1, 1)]
    [InlineData(10, 1)]
    [InlineData(3, 3)]
    [InlineData(4, 3)]
    [InlineData(30, 8)]
    // [InlineData(50, 16)]
    [InlineData(100, 8)]
    // [InlineData(300, 16)]
    public async Task processes_messages_and_produce_output(int numberOfMessages, int partitions) {
        //Arrange
        var processorName     = Fixture.GetUniqueProcessorName("processes_messages_with_output");
        var inputTopic        = await Fixture.CreateInputTopic(processorName, partitions);
        var outputTopic       = await Fixture.CreateOutputTopic(processorName, partitions);
        var producedRecordIds = await Fixture.ProduceTestMessages(inputTopic, numberOfMessages);

        //Act
        var (processedRecords, subscriptionGap) = await Fixture.StreamMessages(
            proc => proc
                .ProcessorName(processorName)
                .InputTopic(inputTopic)
                .OutputTopic(outputTopic),
            numberOfMessages
        );
        
        //Assert
        Fixture.Log.Information("Asserting input was processed and forwarded...");

        try {
            processedRecords.Should().HaveSameCount(producedRecordIds);
        }
        catch (Exception e) {
            throw;
        }
      
        subscriptionGap.First(x => x.Topic == inputTopic).CaughtUp.Should().BeTrue();
        
        Fixture.Log.Information("Asserting output was sent...");
        
        var (processedOutputRecords, subscriptionOutputTopicGap) = await Fixture.ProcessMessages(
            proc => proc
                .ProcessorName($"{processorName}.output-consumer")
                .InputTopic(outputTopic),
            numberOfMessages
        );
        
        subscriptionOutputTopicGap.First(x => x.Topic == outputTopic).CaughtUp.Should().BeTrue();
        processedOutputRecords.Select(x => (x.Key, x.Value))
            .Should().BeEquivalentTo(processedRecords.Select(x => (x.Key, x.Value)));
    }
}