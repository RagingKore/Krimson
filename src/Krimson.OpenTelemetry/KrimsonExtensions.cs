using Krimson.Processors.Configuration;
using Krimson.Producers;

namespace Krimson.OpenTelemetry;

[PublicAPI]
public static class OpenTelemetryExtensions {
    public static KrimsonProcessorBuilder AddOpenTelemetry(this KrimsonProcessorBuilder builder, string? sourceName = null) {
        builder.Intercept(new OpenTelemetryProcessorInterceptor(sourceName ?? builder.Options.ConsumerConfiguration.ClientId));
        return builder;
    }
    
    public static KrimsonProducerBuilder AddOpenTelemetry(this KrimsonProducerBuilder builder, string? sourceName = null) {
        builder.Intercept(new OpenTelemetryProducerInterceptor(sourceName ?? builder.Options.Configuration.ClientId));
        return builder;
    }
}