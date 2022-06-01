using Krimson.Processors;
using Microsoft.Extensions.Logging;
// ReSharper disable CheckNamespace

namespace Krimson.Logging;

public static class LoggerExtensions {
    public static IDisposable WithRecordInfo(this ILogger logger, KrimsonRecord record) {
        var info = new Dictionary<string, object> {
            ["krimson.record.id"]                 = record.Id,
            ["krimson.record.key"]                = record.Key,
            ["krimson.record.position.topic"]     = record.Position.Topic,
            ["krimson.record.position.partition"] = record.Position.Partition.Value,
            ["krimson.record.position.offset"]    = record.Position.Offset.Value,
            ["krimson.record.timestamp"]          = record.Timestamp,
            ["krimson.record.producer"]           = record.ProducerName
        };

        return logger.BeginScope(info);
    }
}