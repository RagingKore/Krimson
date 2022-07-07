using System.Diagnostics;
using Humanizer;

namespace Krimson;

using static System.DateTimeOffset;

static class MicroProfiler {
    public static long GetTimestamp() => UtcNow.ToUnixTimeMilliseconds();
    
    public static long GetElapsedMilliseconds(long start)           => GetTimestamp() - start;
    public static long GetElapsedMilliseconds(DateTimeOffset start) => GetTimestamp() - start.ToUnixTimeMilliseconds();

    public static TimeSpan GetElapsed(long start)           => FromUnixTimeMilliseconds(GetTimestamp()) - FromUnixTimeMilliseconds(start);
    public static TimeSpan GetElapsed(DateTimeOffset start) => FromUnixTimeMilliseconds(GetTimestamp()) - start;

    public static string GetElapsedHumanReadable(long start)           => GetElapsed(start).Humanize(5);
    public static string GetElapsedHumanReadable(DateTimeOffset start) => GetElapsed(start).Humanize(5);
}

public static class StopwatchExtensions {
    public static string GetElapsedHumanReadable(this Stopwatch stopwatch, bool stop = false) {
        if (stop) 
            stopwatch.Stop();

        var ticks   = stopwatch.ElapsedTicks;
        var elapsed = Elapsed(ticks);
        var nanos   = ElapsedNanoseconds(ticks, elapsed.Milliseconds);
        
        return elapsed.Milliseconds == 0
            ? $"{nanos} nanoseconds"
            : nanos > 0
                ? $"{elapsed.Humanize(5)}, {nanos} nanoseconds"
                : $"{elapsed.Humanize(5)}";

        static TimeSpan Elapsed(long ticks) {
            return TimeSpan.FromTicks(ticks / (TimeSpan.TicksPerMillisecond / 100));
        }
        
        static decimal ElapsedNanoseconds(decimal ticks, int elapsedMilliseconds) {
            var subSecondNanos = Convert.ToInt64(ticks / Stopwatch.Frequency * 1000000000m);
            return subSecondNanos - elapsedMilliseconds * 1000000;
        }
    }
}