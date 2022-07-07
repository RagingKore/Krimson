using Confluent.Kafka;
using Serilog.Events;

namespace Krimson.Logging;

static class SysLogLevelConverter {
    public static LogEventLevel Convert(SyslogLevel level) =>
        level switch {
            SyslogLevel.Emergency => LogEventLevel.Fatal,
            SyslogLevel.Alert     => LogEventLevel.Fatal,
            SyslogLevel.Critical  => LogEventLevel.Fatal,
            SyslogLevel.Error     => LogEventLevel.Error,
            SyslogLevel.Warning   => LogEventLevel.Warning,
            SyslogLevel.Notice    => LogEventLevel.Information,
            SyslogLevel.Info      => LogEventLevel.Information,
            SyslogLevel.Debug     => LogEventLevel.Debug,
            _                     => throw new ArgumentOutOfRangeException(nameof(level), level, null)
        };

    public static LogEventLevel GetLogLevel(this LogMessage log) => Convert(log.Level);

    public static int           ToLogLevelValue(SyslogLevel logLevel) => LogLevelLookup[(int)logLevel];
    public static LogEventLevel ToLogLevel(SyslogLevel logLevel)      => (LogEventLevel)LogLevelLookup[(int)logLevel];

    static readonly int[] LogLevelLookup = {
        5, // LogEventLevel.Fatal
        5, // LogEventLevel.Fatal
        5, // LogEventLevel.Fatal
        4, // LogEventLevel.Error
        3, // LogEventLevel.Warning
        2, // LogEventLevel.Information
        2, // LogEventLevel.Information
        1  // LogEventLevel.Debug
    };
}