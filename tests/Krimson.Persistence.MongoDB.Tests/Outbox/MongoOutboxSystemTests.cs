using FluentAssertions;
using Krimson.Fixie;
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
        var result = await outbox.WithTransactionResult(
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

        Context.Services.AddSingleton(new MongoClient("").GetDatabase("krimson"));

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

        await using var processor = serviceProvider.GetRequiredService<IOutboxProcessor>();
        await using var outbox    = serviceProvider.GetRequiredService<MongoOutbox>();

        var result = await outbox.WithTransactionResult(
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


        using var cancellator = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        // ************************************************************
        // Act
        // ************************************************************
        var processedMessages = await processor.ProcessOutbox(strategy, cancellator).ToListAsync(cancellator.Token);

        // ************************************************************
        // Assert
        // ************************************************************

        result.OutboxMessages.Should().BeEquivalentTo(processedMessages, options => options);

        var outboxMessages = await database
            .GetCollection<OutboxMessage>(outboxName)
            .AsQueryable()
            .OrderBy(x => x.ClientId)
            .ThenBy(x => x.SequenceNumber)
            .ToListAsync(cancellator.Token);

        if (strategy == OutboxProcessingStrategy.Delete)
            outboxMessages.Should().BeEmpty();
        else
            outboxMessages.Should().HaveCount(numberOfMessages);
    }
}