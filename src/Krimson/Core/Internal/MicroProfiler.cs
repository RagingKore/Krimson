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