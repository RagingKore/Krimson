// ReSharper disable CheckNamespace

using System;
using System.IO;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Display;
using Krimson;
using Xunit.Abstractions;

namespace Serilog.Sinks.Xunit;

class XunitSink : ILogEventSink {
    internal const string DefaultXunitOutputTemplate
        = "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext} {Message}{NewLine}{Exception}";

    static readonly ITextFormatter DefaultFormatter
        = new MessageTemplateTextFormatter(DefaultXunitOutputTemplate);

    public XunitSink(ITestOutputHelper outputHelper, ITextFormatter? formatter = null) {
        OutputHelper = outputHelper ?? throw new ArgumentNullException(nameof(outputHelper));
        Formatter    = formatter ?? DefaultFormatter;
    }

    ITextFormatter    Formatter    { get; }
    ITestOutputHelper OutputHelper { get; }

    public void Emit(LogEvent logEvent) {
        using var writer = new StringWriter();

        if (logEvent.Exception == null)
            writer.NewLine = string.Empty;

        Formatter.Format(logEvent, writer);
        OutputHelper.WriteLine(writer.ToString());
    }
}

public static class XunitLoggerConfigurationExtensions {
    /// <summary>
    ///     Writes log events to <see cref="ITestOutputHelper" />.
    /// </summary>
    /// <param name="sinkConfiguration">Logger sink configuration.</param>
    /// <param name="outputHelper">Helper to provide Xunit test output.</param>
    /// <param name="restrictedToMinimumLevel">
    ///     The minimum level for events passed through the sink. Ignored when
    ///     <paramref name="levelSwitch" /> is specified.
    /// </param>
    /// <param name="levelSwitch">A switch allowing the pass-through minimum level to be changed at runtime.</param>
    /// <param name="outputTemplate">
    ///     A message template describing the format used to write to the sink, the default is
    ///     <code>"{Timestamp:O} {Level} - {Message}{NewLine}{Exception}"</code>.
    /// </param>
    /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
    /// <returns>Configuration object allowing method chaining.</returns>
    public static LoggerConfiguration Xunit(
        this LoggerSinkConfiguration sinkConfiguration,
        ITestOutputHelper outputHelper,
        LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
        string outputTemplate = XunitSink.DefaultXunitOutputTemplate,
        IFormatProvider? formatProvider = null,
        LoggingLevelSwitch? levelSwitch = null
    ) {
        if (sinkConfiguration == null)
            throw new ArgumentNullException(nameof(sinkConfiguration));

        if (outputHelper == null)
            throw new ArgumentNullException(nameof(outputHelper));

        if (outputTemplate == null)
            throw new ArgumentNullException(nameof(outputTemplate));

        return sinkConfiguration
            .Xunit(
                outputHelper, new MessageTemplateTextFormatter(outputTemplate, formatProvider), restrictedToMinimumLevel,
                levelSwitch
            );
    }

    /// <summary>
    ///     Writes log events to <see cref="ITestOutputHelper" />.
    /// </summary>
    /// <param name="sinkConfiguration">Logger sink configuration.</param>
    /// <param name="outputHelper">Helper to provide Xunit test output.</param>
    /// <param name="formatter">
    ///     Controls the rendering of log events into text, for example to log JSON. To control plain text
    ///     formatting, use the overload that accepts an output template.
    /// </param>
    /// <param name="restrictedToMinimumLevel">
    ///     The minimum level for events passed through the sink. Ignored when
    ///     <paramref name="levelSwitch" /> is specified.
    /// </param>
    /// <param name="levelSwitch">A switch allowing the pass-through minimum level to be changed at runtime.</param>
    /// <returns>Configuration object allowing method chaining.</returns>
    public static LoggerConfiguration Xunit(
        this LoggerSinkConfiguration sinkConfiguration,
        ITestOutputHelper outputHelper,
        ITextFormatter formatter,
        LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
        LoggingLevelSwitch? levelSwitch = null
    ) {
        if (sinkConfiguration == null)
            throw new ArgumentNullException(nameof(sinkConfiguration));

        if (outputHelper == null)
            throw new ArgumentNullException(nameof(outputHelper));

        if (formatter == null)
            throw new ArgumentNullException(nameof(formatter));

        return sinkConfiguration.Sink(new XunitSink(outputHelper, formatter), restrictedToMinimumLevel, levelSwitch);
    }
}

public class XunitOutputSinkProxy : ILogEventSink {
    XunitSink InnerSink { get; set; } = null!;

    public void Emit(LogEvent logEvent) => InnerSink?.Emit(logEvent);

    public void RedirectLogToOutput(ITestOutputHelper output, ITextFormatter? textFormatter = null) => InnerSink = new XunitSink(output, textFormatter);
}

public static class LoggerSinkConfigurationExtension {
    public static LoggerConfiguration XunitOutput(
        this LoggerSinkConfiguration sinkConfiguration,
        XunitOutputSinkProxy xunitSinkProxy,
        LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
        LoggingLevelSwitch? levelSwitch = null
    ) {
        Ensure.NotNull(sinkConfiguration, nameof(sinkConfiguration));
        Ensure.NotNull(xunitSinkProxy, nameof(xunitSinkProxy));

        return sinkConfiguration.Sink(xunitSinkProxy, restrictedToMinimumLevel, levelSwitch);
    }
}