using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Krimson.Tests.Processors;

public class ProcessorTests : IClassFixture<KrimsonProcessorFixture> {
    public ProcessorTests(KrimsonProcessorFixture fixture, ITestOutputHelper output) {
        // Fixture = fixture
        //     .WithOutput<ProcessorTests>(output);

        Fixture = fixture
            .WithOutput<ProcessorTests>(output)
            .WithConnection(
                "pkc-lq8gm.westeurope.azure.confluent.cloud:9092",
                "NKPWCNDO4TLQDSGP",
                "OTPfEtxJwwDFSkru2YHlu/aFCvnTeyP9Cv7U/vZbOSdMLlRdDHVFH56+KLQ++z/I",
                SecurityProtocol.SaslSsl
            )
            .WithSchemaRegistry(
                "https://psrc-0j199.westeurope.azure.confluent.cloud",
                "2BXQG2RDLWSWRGDA",
                "hvlPXp1L4dgRtaE5UOZWCRcXCRpSOxMrN0KQaEVnkRfTL0+uM5Fih84PB4gJUa0I"
            );
    }

    KrimsonProcessorFixture Fixture { get; }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(10, 1)]
    [InlineData(3, 3)]
    [InlineData(4, 3)]
    [InlineData(30, 8)]
    [InlineData(1000, 8)]
    [InlineData(2000, 9)]
    [InlineData(2000, 3)]
    // [InlineData(300, 16)]
    // [InlineData(600, 10)]
    public async Task processes_messages(int numberOfMessages, int partitions) {

        numberOfMessages *= 2;
        
        //Arrange
        var clientId          = Fixture.GenerateUniqueProcessorName();
        var inputTopic        = await Fixture.CreateInputTopic(clientId, partitions);
        var producedRecordIds = await Fixture.ProduceTestMessages(inputTopic, numberOfMessages);
        
        //Act
        var (processedRecords, subscriptionGap) = await Fixture.ProcessMessages(
            proc => proc
                .ClientId(clientId)
                .InputTopic(inputTopic),
            numberOfMessages
        );
        
        //Assert
        Fixture.Log.Information("asserting messages were processed...");

        processedRecords.Should().HaveSameCount(producedRecordIds);
        subscriptionGap.First(x => x.Topic == inputTopic).CaughtUp.Should().BeTrue();
        //processedRecords.Select(x => x.Id).Should().BeEquivalentTo(producedRecordIds);

        Fixture.Log.Information("great success");
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
        var processorName     = Fixture.GenerateUniqueProcessorName();
        var inputTopic        = await Fixture.CreateInputTopic(processorName, partitions);
        var outputTopic       = await Fixture.CreateOutputTopic(processorName, partitions);
        var producedRecordIds = await Fixture.ProduceTestMessages(inputTopic, numberOfMessages);

        //Act
        var (processedRecords, subscriptionGap) = await Fixture.StreamMessages(
            proc => proc
                .ClientId(processorName)
                .InputTopic(inputTopic)
                .OutputTopic(outputTopic),
            numberOfMessages
        );
        
        //Assert
        Fixture.Log.Information("asserting messages were processed and forwarded...");
        
        processedRecords.Should().HaveSameCount(producedRecordIds);
        
        subscriptionGap.First(x => x.Topic == inputTopic).CaughtUp.Should().BeTrue();
        
        Fixture.Log.Information("asserting output was sent...");
        
        var (processedOutputRecords, subscriptionOutputTopicGap) = await Fixture.ProcessMessages(
            proc => proc
                .ClientId($"{processorName}.output-consumer")
                .InputTopic(outputTopic),
            numberOfMessages
        );
        
        subscriptionOutputTopicGap.First(x => x.Topic == outputTopic).CaughtUp.Should().BeTrue();
        // processedOutputRecords.Select(x => (x.Key, x.Value))
        //     .Should().BeEquivalentTo(processedRecords.Select(x => (x.Key, x.Value)));
        
        Fixture.Log.Information("great success");
    }
}