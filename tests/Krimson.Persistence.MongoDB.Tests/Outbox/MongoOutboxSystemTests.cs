using FluentAssertions;
using Krimson.Fixie;
using Krimson.OpenTelemetry;
using Krimson.Persistence.MongoDB.Outbox;
using Krimson.Persistence.Outbox;
using Krimson.Tests.Messages;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace Krimson.Persistence.MongoDB.Tests.Outbox;

public class MongoOutboxSystemTests : TestFixture<MongoDBTestContext> {
    public MongoOutboxSystemTests(MongoDBTestContext context) : base(context) { }

    // [TestCase(250, 1)]
    // // [TestCase(50, 6)]
    // // [TestCase(3, 3)]
    // // [TestCase(4, 3)]
    // // [TestCase(30, 8)]
    // // [TestCase(50, 16)]
    // // [TestCase(100, 8)]
    // // [TestCase(300, 16)]
    // public async Task Processes_Messages_And_Produces_Output_Outbox(int numberOfMessages, int partitions) {
    //     // Arrange
    //     var processorName = Context.GenerateUniqueClientName();
    //     var outputTopic   = await Context.CreateOutputTopic(processorName, partitions);
    //
    //     var mongoClient = new MongoClient("mongodb+srv://elaway:ZAXEONvJQs4od67j@staging.lqup2.mongodb.net/outbox?retryWrites=true&w=majority");
    //     var database    = mongoClient.GetDatabase("krimson");
    //     var serializer  = new ProtobufDynamicSerializer(Context.SchemaRegistry, ProtobufDynamicSerializer.DefaultConfig);
    //
    //     var producer = KrimsonProducer.Builder
    //         .ClientId(processorName)
    //         .Connection(Context.ClientConnection)
    //         .UseProtobuf(Context.SchemaRegistry)
    //         .Create();
    //
    //     await using var outbox          = new MongoOutbox(processorName, database, serializer);
    //     await using var outboxProcessor = new MongoOutboxProcessor(database, producer);
    //
    //     await outbox.WithTransaction(
    //         async ctx => {
    //             for (var i = 1; i <= numberOfMessages; i++) {
    //                 var msg = new KrimsonTestMessage {
    //                     Id        = ObjectId.GenerateNewId().ToString(),
    //                     Order     = i,
    //                     Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
    //                 };
    //
    //                 var order = i.ToString();
    //
    //                 await ctx.ProduceToOutbox(
    //                     request => request
    //                         .Message(msg)
    //                         .Key(msg.Id)
    //                         .Topic(outputTopic)
    //                         .Headers(x => x["order"] = order)
    //                         .Timestamp(msg.Timestamp.ToDateTimeOffset())
    //                 );
    //             }
    //         }
    //     );
    //
    //     await outbox.WithTransaction(
    //         async ctx => {
    //             for (var i = 1; i <= numberOfMessages; i++) {
    //                 var msg = new KrimsonTestMessage {
    //                     Id        = ObjectId.GenerateNewId().ToString(),
    //                     Order     = i,
    //                     Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
    //                 };
    //
    //                 var order = i.ToString();
    //
    //                 await ctx.ProduceToOutbox(
    //                     request => request
    //                         .Message(msg)
    //                         .Key(msg.Id)
    //                         .Topic(outputTopic)
    //                         .Headers(x => x["order"] = order)
    //                         .Timestamp(msg.Timestamp.ToDateTimeOffset())
    //                 );
    //             }
    //         }
    //     );
    //
    //     // var outboxMessagesById             = await database.GetCollection<OutboxMessage>($"{producer.ClientId}-outbox").AsQueryable().OrderBy(x => x.Id).ToListAsync();
    //     // var outboxMessagesByCreateTime     = await database.GetCollection<OutboxMessage>($"{producer.ClientId}-outbox").AsQueryable().OrderBy(x => x.CreatedOn).ToListAsync();
    //     // var outboxMessagesBySequenceNumber = await database.GetCollection<OutboxMessage>($"{producer.ClientId}-outbox").AsQueryable().OrderBy(x => x.SequenceNumber).ToListAsync();
    //     //
    //     // outboxMessagesById.Should().BeEquivalentTo(outboxMessagesByCreateTime, x => x.WithStrictOrderingFor(z => z.CreatedOn));
    //     // outboxMessagesById.Should().BeEquivalentTo(outboxMessagesByCreateTime, x => x.WithStrictOrderingFor(z => z.Id));
    //     // outboxMessagesByCreateTime.Should().BeEquivalentTo(outboxMessagesBySequenceNumber, x => x.WithStrictOrderingFor(z => z.SequenceNumber));
    //
    //     var processedMessages = await outbox.ProcessOutbox(OutboxProcessingStrategy.Delete, CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None)).ToListAsync();
    //
    //     var sequenceNumber = await database.GetCurrentSequenceNumber(producer.ClientId);
    //
    //     var count = await database.GetCollection<OutboxMessage>($"{producer.ClientId}-outbox").AsQueryable().Where(x => x.ProcessedOn == null).CountAsync();
    //
    //     //var count = await database.GetCollection<OutboxMessage>($"{producer.ClientId}-outbox").AsQueryable().CountAsync();
    //
    //     count.Should().Be(0);
    // }

    [TestCase(1)]
    [TestCase(100)]
    public async Task produces_messages_to_outbox(int numberOfMessages) {
        // ************************************************************
        // Arrange
        // ************************************************************
        var clientName  = Context.GenerateUniqueClientName();
        var outputTopic = $"{clientName}-destination.tst";
        var outboxName  = $"{clientName}-outbox";

        var services = new ServiceCollection();

        services.AddSingleton(new MongoClient("mongodb+srv://elaway:ZAXEONvJQs4od67j@staging.lqup2.mongodb.net/outbox?retryWrites=true&w=majority").GetDatabase("krimson"));

        services.AddKrimson()
            .AddSchemaRegistry(Context.SchemaRegistry)
            .UseConfluentProtobuf()
            .AddMongoOutbox(clientName);

        var serviceProvider = services.BuildServiceProvider();

        var database = serviceProvider.GetRequiredService<IMongoDatabase>();

        await using var outbox = serviceProvider.GetRequiredService<MongoOutbox>();

        // ************************************************************
        // Act
        // ************************************************************
        var requests = await outbox.WithTransaction(
            async ctx => {
                for (var i = 1; i <= numberOfMessages; i++) {
                    var msg = new KrimsonTestMessage {
                        Id        = ObjectId.GenerateNewId().ToString(),
                        Order     = i,
                        Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
                    };

                    await ctx.ProduceToOutbox(
                        request => request
                            .Message(msg)
                            .Key(msg.Id)
                            .Topic(outputTopic)
                            .Headers(x => x["order"] = msg.Order.ToString())
                            .Timestamp(msg.Timestamp.ToDateTimeOffset())
                    );
                }
            }
        );

        // ************************************************************
        // Assert
        // ************************************************************
        var outboxMessages = await database
            .GetCollection<OutboxMessage>(outboxName)
            .AsQueryable()
            .OrderBy(x => x.ClientId)
            .ThenBy(x => x.SequenceNumber)
            .ToListAsync();

        outboxMessages.Should().HaveCount(numberOfMessages);
    }

    [TestCase(10, OutboxProcessingStrategy.Delete)]
    [TestCase(10, OutboxProcessingStrategy.Update)]
    public async Task processes_outbox(int numberOfMessages, OutboxProcessingStrategy strategy) {
        // ************************************************************
        // Arrange
        // ************************************************************
        var clientName  = Context.GenerateUniqueClientName();
        var outputTopic = $"{clientName}-destination.tst";
        var outboxName  = $"{clientName}-outbox";

        Context.Services.AddSingleton(new MongoClient("mongodb+srv://elaway:ZAXEONvJQs4od67j@staging.lqup2.mongodb.net/outbox?retryWrites=true&w=majority").GetDatabase("krimson"));

        Context.Services.AddKrimson()
            .AddSchemaRegistry(Context.SchemaRegistry)
            .UseConfluentProtobuf()
            .AddProducer(
                prd => prd
                    .Connection(Context.ClientConnection)
                    .ClientId(clientName)
            )
            .AddMongoOutbox(clientName)
            .AddMongoOutboxProcessor();

        var serviceProvider = Context.Services.BuildServiceProvider();

        var database = serviceProvider.GetRequiredService<IMongoDatabase>();

        await using var outbox = serviceProvider.GetRequiredService<MongoOutbox>();

        var persistedMessages = await outbox.WithTransaction(
            async ctx => {
                for (var i = 1; i <= numberOfMessages; i++) {
                    var msg = new KrimsonTestMessage {
                        Id        = ObjectId.GenerateNewId().ToString(),
                        Order     = i,
                        Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
                    };

                    await ctx.ProduceToOutbox(
                        request => request
                            .Message(msg)
                            .Key(msg.Id)
                            .Topic(outputTopic)
                            .Headers(x => x["order"] = msg.Order.ToString())
                            .Timestamp(msg.Timestamp.ToDateTimeOffset())
                    );
                }
            }
        );

        // ************************************************************
        // Act
        // ************************************************************
        var processor = serviceProvider.GetRequiredService<IOutboxProcessor>();

        using var cancellator = new CancellationTokenSource(TimeSpan.FromMinutes(1));

        var processedMessages = await processor.ProcessOutbox(OutboxProcessingStrategy.Delete, cancellator).ToListAsync(cancellator.Token);

        // ************************************************************
        // Assert
        // ************************************************************

        persistedMessages.Should().BeEquivalentTo(processedMessages, options => options);

        var outboxMessages = await database
            .GetCollection<OutboxMessage>(outboxName)
            .AsQueryable()
            .OrderBy(x => x.ClientId)
            .ThenBy(x => x.SequenceNumber)
            .ToListAsync(cancellator.Token);

        if (strategy == OutboxProcessingStrategy.Delete)
            outboxMessages.Should().BeEmpty();
        else
            outboxMessages.Should().HaveCount(0);

        // await outbox.DisposeAsync();
        // await processor.DisposeAsync();

        cancellator.Cancel();
        cancellator.Dispose();
    }
}