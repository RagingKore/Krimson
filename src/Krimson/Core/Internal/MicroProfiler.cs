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
    

    // public static long GetNanoseconds(this Stopwatch stopwatch) {
    //     double timestamp   = stopwatch.GetTimestamp();
    //     double nanoseconds = 1_000_000_000.0 * timestamp / Stopwatch.Frequency;
    //
    //     return (long)nanoseconds;
    // }

}
//
// public static class TimeSpanExtension {
//     const decimal TicksPerNanosecond = TimeSpan.TicksPerMillisecond / 1000000m;
//     
//     public static decimal GetNanoseconds(this TimeSpan ts)         => ts.Ticks / TicksPerNanosecond;
//     public static decimal GetMicroAndNanoSeconds(this TimeSpan ts) => ts.Ticks % TimeSpan.TicksPerMillisecond / TicksPerNanosecond;
//
// }

public static class StopwatchExtensions{
    // const decimal TimeSpanTicksPerNanosecond = TimeSpan.TicksPerMillisecond / 1000000m;
    // public static decimal ElapsedSubsecondNanoseconds(this Stopwatch stopwatch) {
    //     decimal timestamp   = stopwatch.ElapsedTicks;
    //     decimal nanoseconds = timestamp / Stopwatch.Frequency;
    //     return Convert.ToInt64(nanoseconds * 1000000000m);
    // }
    //
    // public static decimal ElapsedNanoseconds(this Stopwatch stopwatch) {
    //     var nanos = ElapsedSubsecondNanoseconds(stopwatch);
    //     return nanos - stopwatch.ElapsedMilliseconds * 1000000;
    // }
    //
    // public static string GetElapsedHumanReadable(this Stopwatch stopwatch) {
    //     stopwatch.Stop();
    //
    //     var nanos   = stopwatch.ElapsedNanoseconds();
    //     var elapsed = TimeSpan.FromTicks(stopwatch.ElapsedTicks / (TimeSpan.TicksPerMillisecond / 100));
    //
    //     var v1 = stopwatch.Elapsed.Humanize(5);
    //     var v2 = elapsed.Humanize(5);
    //     
    //     return stopwatch.ElapsedMilliseconds == 0 
    //         ? $"{nanos} nanoseconds" 
    //         : nanos > 0 
    //             ? $"{stopwatch.Elapsed.Humanize(5)}, {nanos} nanoseconds"
    //             : $"{stopwatch.Elapsed.Humanize(5)}";
    // }

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

    // public static string GetElapsedHumanReadable(this Stopwatch stopwatch) {
    //     var ticks = stopwatch.ElapsedTicks;
    //
    //     var nanos   = stopwatch.ElapsedNanoseconds();
    //     var elapsed = TimeSpan.FromTicks(stopwatch.ElapsedTicks / (TimeSpan.TicksPerMillisecond / 100));
    //
    //     var v1 = stopwatch.Elapsed.Humanize(5);
    //     var v2 = elapsed.Humanize(5);
    //
    //     return stopwatch.ElapsedMilliseconds == 0
    //         ? $"{nanos} nanoseconds"
    //         : nanos > 0
    //             ? $"{stopwatch.Elapsed.Humanize(5)}, {nanos} nanoseconds"
    //             : $"{stopwatch.Elapsed.Humanize(5)}";
    //
    //     static decimal ElapsedNanoseconds(decimal ticks, long elapsedMillis) {
    //         var nanoseconds = Convert.ToInt64(ticks / Stopwatch.Frequency * 1000000000m);
    //         return nanoseconds - elapsedMillis * 1000000;
    //     }
    // }
}
