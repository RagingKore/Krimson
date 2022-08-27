using Krimson.Processors.Configuration;
using Krimson.Producers;

namespace Krimson.OpenTelemetry;

[PublicAPI]
public static class OpenTelemetryExtensions {
    public static KrimsonProcessorBuilder AddOpenTelemetry(this KrimsonProcessorBuilder builder, string sourceName) {
        builder.Intercept(new OpenTelemetryProcessorInterceptor(sourceName));
        return builder;
    }
    
    public static KrimsonProducerBuilder AddOpenTelemetry(this KrimsonProducerBuilder builder, string sourceName) {
        builder.Intercept(new OpenTelemetryProducerInterceptor(sourceName));
        return builder;
    }
}