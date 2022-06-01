// ReSharper disable TemplateIsNotCompileTimeConstantProblem

using System.Reflection;
using Fixie;
using JetBrains.Annotations;
using Serilog;
using Serilog.Context;
using Serilog.Core;
using Serilog.Core.Enrichers;
using Serilog.Events;
using Serilog.Exceptions;
using Serilog.Exceptions.Filters;
using Serilog.Filters;
using Serilog.Sinks.SystemConsole.Themes;
using SerilogTimings;
using SerilogTimings.Extensions;
using static System.Activator;

namespace Krimson.Fixie; 

[PublicAPI]
public abstract class DefaultTestProjectSetup : ITestProject {
    const string LogOutputTemplate = "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext}{NewLine}{Message}{NewLine}{Exception}";

    public void Configure(TestConfiguration configuration, TestEnvironment environment) {
        Log.Logger = ConfigureLogger(
                new LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
            )
            .Enrich.WithProperty("SourceContext", nameof(Fixie))
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .Enrich.WithExceptionDetails()
            .WriteTo.Logger(
                logger => logger
                    //.Filter.ByExcluding(Matching.FromSource(nameof(Fixie)))
                    .WriteTo.Console(theme: AnsiConsoleTheme.Literate, outputTemplate: LogOutputTemplate, applyThemeToRedirectedOutput: true)
            )
            .CreateLogger();
        
        configuration.Conventions.Add(
            new KrimsonTestDiscovery(), 
            new KrimsonTestExecution()
        );
    }

    public virtual LoggerConfiguration ConfigureLogger(LoggerConfiguration configuration) => configuration;
}

class KrimsonTestDiscovery : IDiscovery {
    static readonly ILogger Logger = Log.ForContext("SourceContext", nameof(Fixie));
    
    public IEnumerable<Type> TestClasses(IEnumerable<Type> concreteClasses) {
        return concreteClasses
            .Where(x => x.IsAssignableTo(typeof(ITestFixture)))
            .OrderBy(x => x.FullName)
            .Select(
                x => {
                    Logger.Verbose("{Discovery} test fixture found: {TestFixture}", nameof(KrimsonTestDiscovery), x.FullName);
                    return x;
                }
            ).ToList();
    }

    public IEnumerable<MethodInfo> TestMethods(IEnumerable<MethodInfo> publicMethods) =>
        publicMethods
            .Where(x => x.Has<TestAttribute>() || x.Has<TestCaseAttribute>() || x.Has<InlineDataAttribute>())
            .Shuffle();
}

class KrimsonTestExecution : IExecution {
    static readonly ILogger Logger = Log.ForContext("SourceContext", nameof(Fixie));

    public async Task Run(TestSuite testSuite) {
        var testRunId = Guid.NewGuid().ToString();

        using (LogContext.PushProperty("TestRunId", testRunId)) {
            Logger.Verbose("test run {TestRunId} {Operation}", testRunId, "starting");

            foreach (var testClass in testSuite.TestClasses)
            foreach (var test in testClass.Tests) {
                await ExecuteTest(test, testClass).ConfigureAwait(false);
            }

            Logger.Verbose("test run {TestRunId} {Operation}", testRunId, "completed");
        }
    }
    
    static async Task ExecuteTest(Test test, TestClass testClass) {
        if (!test.HasParameters)
            await RunTest().ConfigureAwait(false);
        else {
            foreach (var parameters in test.GetAll<InlineDataAttribute>().Select(x => x.Parameters))
                await RunTest(parameters).ConfigureAwait(false);
        }

        async Task RunTest(params object[] parameters) {
            try {
                var testCaseName = TestCaseName.From(test, parameters);

                Logger.Verbose("test {TestCaseName} {Operation}", testCaseName, "executing");
                
                ILogEventEnricher[] props = {
                    new PropertyEnricher("SourceContext", testCaseName),
                    new PropertyEnricher("TestFixture", testClass.Type.Name),
                    new PropertyEnricher("TestName", test.Name),
                    new PropertyEnricher("TestCaseName", testCaseName)
                };

                using (LogContext.Push(props)) {
           
                    var operation = Logger
                        .OperationAt(LogEventLevel.Information, LogEventLevel.Error)
                        .Begin("test {TestCaseName}", testCaseName);

                    using (operation) {
                        var testFixture = CreateTestFixtureInstance(testClass);

                        await InitializeTestFixture(testFixture, testCaseName).ConfigureAwait(false);
                        
                        var result = await test
                            .Run(testFixture, parameters)
                            .ConfigureAwait(false);

                        if (result is Failed failure)
                            operation.Abandon(failure.Reason);
                        else
                            operation.Complete();

                        await DisposeTestFixture(testFixture, testCaseName).ConfigureAwait(false); 
                    }
                }
            }
            catch (Exception _) {
                // i do see the glitches in the matrix
            }

            // using (LogContext.Push(props)) {
            //     var operation = Logger
            //         .OperationAt(LogEventLevel.Information, LogEventLevel.Error)
            //         .Begin("test {TestCaseName}", testCaseName);
            //
            //     using (operation) {
            //         try {
            //             var testFixture = CreateTestFixtureInstance(testClass);
            //
            //             await InitializeTestFixture(testFixture, testCaseName).ConfigureAwait(false);
            //         
            //             var result = await test
            //                 .Run(testFixture, parameters)
            //                 .ConfigureAwait(false);
            //
            //             if (result is Failed failure)
            //                 operation.Abandon(failure.Reason);
            //             else
            //                 operation.Complete();
            //
            //             await DisposeTestFixture(testFixture, testCaseName).ConfigureAwait(false);
            //         }
            //         catch (Exception ex) {
            //             operation.Abandon(ex);
            //         }
            //     }
            // }
        }
    }

    static ITestFixture CreateTestFixtureInstance(TestClass testClass) {
        var context = CreateTestContextInstance(testClass);

        return (ITestFixture)testClass.Construct(context);

        static ITestContext? CreateTestContextInstance(TestClass testClass) {
            var contextType = testClass.Type.GetInterfaces()
                .FirstOrDefault(type => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ITestFixture<>))
                ?.GetGenericArguments()[0];

            return contextType is not null ? (ITestContext)CreateInstance(contextType)! : null;
        }
    }
    
    static async Task InitializeTestFixture(ITestFixture fixture, string testCaseName) {
        var contextDisplayName = fixture.TestContext?.GetType().Name;

        var template = contextDisplayName is not null
            ? "test {TestCaseName} {ContextName} {Operation}"
            : "test {TestCaseName} {Operation}";

        try {
            Logger.Verbose(template, testCaseName, contextDisplayName, "initializing");
            
            if (fixture.TestContext is not null)
                await fixture.TestContext
                    .InitializeAsync()
                    .ConfigureAwait(false);
            else
                await fixture
                    .InitializeAsync()
                    .ConfigureAwait(false);

            Logger.Verbose(template, testCaseName, contextDisplayName, "initialized");
        }
        catch (Exception ex) {
            Logger.Verbose(ex, template, testCaseName, contextDisplayName, "failed");
            throw;
        }
    }

    static async Task DisposeTestFixture(ITestFixture fixture, string testCaseName) {
        var contextDisplayName = fixture.TestContext?.GetType().Name;

        var template = contextDisplayName is not null
            ? "test {TestCaseName} {ContextName} {Operation}"
            : "test {TestCaseName} {Operation}";

        try {
            Logger.Verbose(template, testCaseName, contextDisplayName, "disposing");

    
            if (fixture.TestContext is not null)
                await fixture.TestContext
                    .DisposeAsync()
                    .ConfigureAwait(false);
            else
                await fixture
                    .DisposeAsync()
                    .ConfigureAwait(false);

            Logger.Verbose(template, testCaseName, contextDisplayName, "disposed");
        }
        catch (Exception ex) {
            Logger.Verbose(ex, template, testCaseName, contextDisplayName, "failed");
            throw;
        }
    }
}
