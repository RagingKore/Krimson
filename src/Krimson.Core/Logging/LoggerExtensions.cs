using Krimson.Processors;
using Serilog;
using Serilog.Core.Enrichers;

namespace Krimson.Logging;

static class LoggerExtensions {
    public static ILogger WithRecordInfo(this ILogger logger, KrimsonRecord record) {
        var info = new[] {
            new PropertyEnricher("krimson.record.id", record.Id),
            new PropertyEnricher("krimson.record.key", record.Key),
            new PropertyEnricher("krimson.record.position.topic", record.Position.Topic),
            new PropertyEnricher("krimson.record.position.partition", record.Position.Partition.Value),
            new PropertyEnricher("krimson.record.position.offset", record.Position.Offset.Value),
            new PropertyEnricher("krimson.record.timestamp", record.Timestamp),
            new PropertyEnricher("krimson.record.producer", record.Id, true),
        };
        
        return logger.ForContext(info);
    }
}