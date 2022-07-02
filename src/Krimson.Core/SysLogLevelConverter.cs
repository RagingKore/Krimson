using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Krimson;

public static class SysLogLevelConverter {
    public static LogLevel Convert(SyslogLevel level) =>
        level switch {
            SyslogLevel.Emergency => LogLevel.Critical,
            SyslogLevel.Alert     => LogLevel.Critical,
            SyslogLevel.Critical  => LogLevel.Critical,
            SyslogLevel.Error     => LogLevel.Error,
            SyslogLevel.Warning   => LogLevel.Warning,
            SyslogLevel.Notice    => LogLevel.Information,
            SyslogLevel.Info      => LogLevel.Information,
            SyslogLevel.Debug     => LogLevel.Debug,
            _                     => throw new ArgumentOutOfRangeException(nameof(level), level, null)
        };

    public static LogLevel GetLogLevel(this LogMessage log) => Convert(log.Level);

    public static int      ToLogLevelValue(SyslogLevel logLevel) => LogLevelLookup[(int)logLevel];
    public static LogLevel ToLogLevel(SyslogLevel logLevel)      => (LogLevel)LogLevelLookup[(int)logLevel];

    static readonly int[] LogLevelLookup = {
        5, // LogLevel.Critical
        5, // LogLevel.Critical
        5, // LogLevel.Critical
        4, // LogLevel.Error
        3, // LogLevel.Warning
        2, // LogLevel.Information
        2, // LogLevel.Information
        1  // LogLevel.Debug
    };
}