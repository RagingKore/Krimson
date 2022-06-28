using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Krimson.Logging;

static class Log {
    static ILoggerFactory Factory { get; set; } = new NullLoggerFactory();

    public static void SetLoggerFactory(ILoggerFactory factory) {
        if (Factory is NullLoggerFactory)
            Factory = factory;
    }

    public static ILogger CreateLogger(string categoryName) => Factory.CreateLogger(categoryName);
    public static ILogger CreateLogger(Type type)           => Factory.CreateLogger(type.Name);
    public static ILogger CreateLogger<T>()                 => Factory.CreateLogger<T>();
    
    public static void WithState(this ILogger logger, Dictionary<string, object> state, Action<ILogger> log) {
        using var disposable = logger.BeginScope(state);
        log(logger);
    }
}    