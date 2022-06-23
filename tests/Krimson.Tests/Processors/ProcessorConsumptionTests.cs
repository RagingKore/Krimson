using Confluent.Kafka;
using FluentAssertions;
using Krimson.Fixie;

namespace Krimson.Tests; 

public class ProcessorConsumptionTests : TestFixture<KrimsonTestContext> {
    public ProcessorConsumptionTests(KrimsonTestContext context) : base(context) { }
    
    [TestCase(1, 1)]
    [TestCase(10, 1)]
    public Task ConsumesFromTopic(int numberOfMessages, int partitions) => ConsumeMessages(numberOfMessages, partitions, true);
    
    [TestCase(3, 3)]
    [TestCase(30, 2)]
    [TestCase(30, 3)]
    public Task ConsumesFromPartitionedTopic(int numberOfMessages, int partitions) => ConsumeMessages(numberOfMessages, partitions);
    
    async Task ConsumeMessages(int numberOfMessages, int partitions, bool checkMessage = false) {
        // Arrange
        var clientId          = Context.GenerateUniqueProcessorName();
        var inputTopic        = await Context.CreateInputTopic(clientId, partitions);
        var producedRecordIds = await Context.ProduceTestMessages(inputTopic, numberOfMessages);
        
        // Act
        var (processedRecords, subscriptionGap) = await Context.ProcessMessages(
            proc => proc
                .ClientId(clientId)
                .InputTopic(inputTopic),
            numberOfMessages
        );
        
        // Assert
        processedRecords.Should().HaveSameCount(producedRecordIds);
        
        subscriptionGap.First(x => x.Topic == inputTopic).CaughtUp
            .Should().BeTrue();
        
        if(checkMessage) 
            processedRecords.Select(x => x.Id).Should().BeEquivalentTo(producedRecordIds);
    }
}