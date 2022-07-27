using Confluent.SchemaRegistry;
using Krimson.SchemaRegistry;
using Krimson.Serializers.ConfluentProtobuf;
using Microsoft.Extensions.Hosting;
using Serilog;
using static Serilog.Core.Constants;

namespace Krimson.Connectors;

public class BackgroundPullSourceConnectorService<T> : BackgroundService where T : PullSourceConnector {
    public BackgroundPullSourceConnectorService(T connector, ISchemaRegistryClient schemaRegistry) {
        Connector      = connector;
        SchemaRegistry = schemaRegistry;
    }

    T                     Connector      { get; }
    ISchemaRegistryClient SchemaRegistry { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var messageSchema = await SchemaRegistry.RegisterMessage(SourceRecord.Descriptor);
        
        Log
            .ForContext(SourceContextPropertyName, "BackgroundPullSourceConnectorService")
            .ForContext(nameof(MessageSchema), messageSchema, true)
            .Debug("source record message schema registered");

        await Connector.Execute(stoppingToken).ConfigureAwait(false);
    }
}