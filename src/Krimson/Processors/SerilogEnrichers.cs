// ReSharper disable CheckNamespace

using Serilog.Core;
using Serilog.Events;
using Krimson.Processors;

namespace Serilog.Enrichers;

class RecordEnricher : ILogEventEnricher {
    internal RecordEnricher(KrimsonRecord record) => Record = record;

    KrimsonRecord Record { get; }

    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory) =>
        logEvent
            .WithScalarProperty("krimson.record.id", Record.Id)
            .WithScalarProperty("krimson.record.key", Record.Key)
            .WithScalarProperty("krimson.record.position.topic", Record.Position.Topic)
            .WithScalarProperty("krimson.record.position.partition", Record.Position.Partition.Value)
            .WithScalarProperty("krimson.record.position.offset", Record.Position.Offset.Value)
            .WithScalarProperty("krimson.record.timestamp", Record.Timestamp)
            .WithScalarProperty("krimson.record.producer", Record.ProducerName);
}

// public class ProcessorInfoEnricher : ILogEventEnricher {
//     internal ProcessorInfoEnricher(KafkaProcessor processor) => Processor = processor;
//
//     KafkaProcessor Processor { get; }
//
//     public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory) =>
//         logEvent
//             .WithScalarProperty("krimson.processor.name", Processor.ProcessorName)
//             .WithScalarProperty("krimson.processor.subscription", Processor.SubscriptionName);
// }

static class LoggerEnrichmentConfigurationExtensions {
    internal static ILogger WithRecordInfo(this ILogger source, KrimsonRecord record) =>
        source.ForContext(new RecordEnricher(record));

    // internal static LoggerConfiguration WithProcessorInfo(this LoggerEnrichmentConfiguration source, KafkaProcessor processor) =>
    //     source.With(new ProcessorInfoEnricher(processor));
    //
    // internal static LoggerConfiguration WithRecordInfo(this LoggerEnrichmentConfiguration source, KafkaRecord record) =>
    //     source.With(new RecordEnricher(record));
    //
    // internal static ILogger WithProcessorInfo(this ILogger source, KafkaProcessor processor) => source.ForContext(new ProcessorInfoEnricher(processor));
}

static class LogEventExtensions {
    internal static LogEvent WithScalarProperty(this LogEvent source, string name, object value) {
        source.AddPropertyIfAbsent(new LogEventProperty(name, new ScalarValue(value)));
        return source;
    }
}