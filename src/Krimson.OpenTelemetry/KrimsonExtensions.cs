using Krimson.Processors.Configuration;
using Krimson.Producers;
using OpenTelemetry.Trace;

namespace Krimson.OpenTelemetry;

[PublicAPI]
public static class OpenTelemetryExtensions {
    public static KrimsonProcessorBuilder AddOpenTelemetry(this KrimsonProcessorBuilder builder, string? sourceName = null) =>
        builder.Intercept(new OpenTelemetryProcessorInterceptor(string.IsNullOrWhiteSpace(sourceName) ? nameof(Krimson) : sourceName));

    public static KrimsonProducerBuilder AddOpenTelemetry(this KrimsonProducerBuilder builder, string? sourceName = null) =>
        builder.Intercept(new OpenTelemetryProducerInterceptor(string.IsNullOrWhiteSpace(sourceName) ? nameof(Krimson) : sourceName));

    public static TracerProviderBuilder AddKrimsonSource(this TracerProviderBuilder builder, string? sourceName = null) =>
        builder.AddSource(string.IsNullOrWhiteSpace(sourceName) ? nameof(Krimson) : sourceName);
}