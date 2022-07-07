using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using OpenTelemetry.Trace;

namespace Krimson.OpenTelemetry.Trace;

static class ActivityExtensions {
    public static Activity InjectHeaders(this Activity activity, IDictionary<string, string?> headers) {
        Propagators.DefaultTextMapPropagator.Inject(
            new PropagationContext(activity.Context, Baggage.Current),
            headers, (carrier, key, value) => carrier[key] = value
        );

        return activity;
    }
    
    public static Activity SetException(this Activity activity, Exception? exception) {
        if (exception is null) return activity;

        if (exception is AggregateException aex)
            activity.RecordException(aex.Flatten());
        else
            activity.RecordException(exception);
        
        return activity;
    }
  
    public static Activity SetOptionalTag<T>(this Activity activity, string key, T value)
        => value is null ? activity : activity.SetTag(key, value as string ?? value.ToString()!);
}

static class HeadersExtensions {
    public static PropagationContext ExtractPropagationContext(this IDictionary<string, string?> headers) {
        return Propagators.DefaultTextMapPropagator.Extract(
            new PropagationContext(), headers, 
            (carrier, key) => carrier.TryGetValue(key, out var value)
                ? new[] { value }
                : Array.Empty<string>()
        );
    }
}