using Confluent.Kafka;
using FluentAssertions;
using Google.Protobuf.WellKnownTypes;
using Krimson.Connectors;
using Krimson.Fixie;
using Krimson.Producers;
using Krimson.Serializers.ConfluentProtobuf;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace Krimson.Tests.SchemaRegistry; 

public class SchemaRegistryTests : TestFixture<KrimsonTestContext> {
    public SchemaRegistryTests(KrimsonTestContext context) : base(context) { }

    [Test]
    public async Task RegistersProtobufSchema() {
        var clientId   = Context.GenerateUniqueProcessorName();
        var inputTopic = "elw.platform.device-registry.sources.volte";

        var schemas = await Context.SchemaRegistry.GetAllSubjectsAsync();
        
        await using var producer = KrimsonProducer.Builder
            .Connection(Context.ClientConnection)
            .UseProtobuf(Context.SchemaRegistry)
            .ClientId(clientId)
            .Topic(inputTopic)
            .Create();

        var sourceRecord = new SourceRecord {
            Id        = Guid.NewGuid().ToString(),
            Data      = new Struct(),
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
            Type      = "none",
            Operation = SourceOperation.Snapshot,
            Source    = "tests"
        };
        
        await producer.Produce(sourceRecord.ToProduceRequest());
        
        var (processedRecords, subscriptionGap) = await Context.ProcessMessages(
            proc => proc
                .ClientId(clientId)
                .InputTopic(inputTopic),
            1
        );
        
        processedRecords.Should().HaveCount(1);
    }
    
}