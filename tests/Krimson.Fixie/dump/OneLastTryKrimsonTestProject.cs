// using System.Reflection;
// using Fixie;
// using JetBrains.Annotations;
// using Serilog;
// using Serilog.Context;
// using Serilog.Core;
// using Serilog.Core.Enrichers;
// using Serilog.Events;
// using Serilog.Exceptions;
// using Serilog.Exceptions.Filters;
// using Serilog.Filters;
// using Serilog.Sinks.SystemConsole.Themes;
// using SerilogTimings;
// using SerilogTimings.Extensions;
// using static System.Activator;
//
// namespace Krimson.Fixie; 
//
// [PublicAPI]
// public abstract class KrimsonTestProject2 : ITestProject {
//     const string LogOutputTemplate = "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext}{NewLine}{Message}{NewLine}{Exception}";
//
//     public void Configure(TestConfiguration configuration, TestEnvironment environment) =>
//         configuration.Conventions.Add(new KrimsonTestDiscovery(), new KrimsonTestExecution(ConfigureLogger));
//
//     public virtual void ConfigureLogger(LoggerConfiguration configuration) { }
// }
//
// [PublicAPI]
// public abstract class KrimsonTestProject : ITestProject {
//     const string LogOutputTemplate = "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext}{NewLine}{Message}{NewLine}{Exception}";
//
//     public void Configure(TestConfiguration configuration, TestEnvironment environment) =>
//         configuration.Conventions.Add(new KrimsonTestDiscovery(), new KrimsonTestExecution(ConfigureLogger));
//
//     public virtual void ConfigureLogger(LoggerConfiguration configuration) { }
//
//     // class CustomDiscovery : IDiscovery {
//     //     static readonly string[] LifecycleMethods = {
//     //         "InitializeAsync",
//     //         "DisposeAsync"
//     //     };
//     //
//     //     public IEnumerable<Type> TestClasses(IEnumerable<Type> concreteClasses) => 
//     //         concreteClasses.Where(x => x.Name.EndsWith("Tests") || x.Has<TestFixtureAttribute>());
//     //
//     //     public IEnumerable<MethodInfo> TestMethods(IEnumerable<MethodInfo> publicMethods) =>
//     //         publicMethods
//     //             .Where(x => !LifecycleMethods.Contains(x.Name))
//     //             .Where(x => x.Has<TestAttribute>() || x.Has<InlineDataAttribute>())
//     //             .Shuffle();
//     // }
//     //
//     // class CustomDiscovery : IDiscovery {
//     //     public IEnumerable<Type> TestClasses(IEnumerable<Type> concreteClasses) =>
//     //         concreteClasses.Where(x => x.IsAssignableTo(typeof(ITestFixture)) || x.Has<TestFixtureAttribute>());
//     //
//     //     public IEnumerable<MethodInfo> TestMethods(IEnumerable<MethodInfo> publicMethods) =>
//     //         publicMethods
//     //             .Where(x => x.Has<TestAttribute>() || x.Has<TestCaseAttribute>() || x.Has<InlineDataAttribute>())
//     //             .Shuffle();
//     // }
//     //
//     // class CustomExecution : IExecution {
//     //     Func<LoggerConfiguration, LoggerConfiguration> ConfigureLogger { get; }
//     //
//     //     public CustomExecution(Action<LoggerConfiguration> configureLogger) {
//     //         ConfigureLogger = loggerConfiguration => {
//     //             configureLogger(loggerConfiguration);
//     //             return loggerConfiguration;
//     //         };
//     //     }
//     //     
//     //     public async Task Run(TestSuite testSuite) {
//     //         Log.Logger = ConfigureLogger(new LoggerConfiguration().MinimumLevel.Debug())
//     //             .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
//     //             .Enrich.FromLogContext()
//     //             .Enrich.WithThreadId()
//     //             .Enrich.WithExceptionDetails()
//     //             .Enrich.WithProperty("SourceContext", nameof(Fixie))
//     //             .WriteTo.Logger(
//     //                 logger => logger
//     //                     .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
//     //                     .WriteTo.Seq("http://localhost:5341")
//     //             )
//     //             .WriteTo.Logger(
//     //                 logger => logger
//     //                     //.Filter.ByExcluding(Matching.FromSource(nameof(Fixie)))
//     //                     .WriteTo.Console(theme: AnsiConsoleTheme.Literate, outputTemplate: LogOutputTemplate, applyThemeToRedirectedOutput: true)
//     //             )
//     //             .CreateLogger();
//     //         
//     //         try {
//     //             var testRunId = Guid.NewGuid().ToString();
//     //             
//     //             using (LogContext.PushProperty("TestRunId", testRunId)) {
//     //                 Log.Verbose("starting test run {TestRunId}", testRunId);
//     //                 
//     //                 foreach (var testClass in testSuite.TestClasses)
//     //                 foreach (var test in testClass.Tests) {
//     //                     await Execute(test, testClass).ConfigureAwait(false);
//     //                 }
//     //             }
//     //         }
//     //         finally {
//     //             Log.CloseAndFlush();
//     //         }
//     //
//     //         static async Task Execute(Test test, TestClass testClass) {
//     //             if (!test.HasParameters)
//     //                 await RunTest().ConfigureAwait(false);
//     //             else {
//     //                 foreach (var parameters in test.GetAll<InlineDataAttribute>().Select(x => x.Parameters))
//     //                     await RunTest(parameters).ConfigureAwait(false);
//     //             }
//     //
//     //             async Task RunTest(params object[] parameters) {
//     //                 var testCaseName = TestCaseName.From(test, parameters);
//     //                 
//     //                 Log.ForContext("SourceContext", nameof(Fixie))
//     //                     .Verbose("Test {TestCaseName} {Operation}", testCaseName, "executing");
//     //                 
//     //                 ILogEventEnricher[] props = {
//     //                     new PropertyEnricher("SourceContext", testCaseName),
//     //                     new PropertyEnricher("TestClass", testClass.Type.Name),
//     //                     new PropertyEnricher("TestName", test.Name),
//     //                     new PropertyEnricher("TestCaseName", testCaseName)
//     //                 };
//     //
//     //                 using (LogContext.Push(props)) {
//     //                     var operation = Log
//     //                         .ForContext("SourceContext", nameof(Fixie))
//     //                         .OperationAt(LogEventLevel.Information, LogEventLevel.Error)
//     //                         .Begin("Test {TestCaseName}", testCaseName);
//     //
//     //                     using (operation) {
//     //                         var testFixture = testClass.CreateTestFixtureInstance();
//     //
//     //                         await testFixture
//     //                             .InitializeAsync(testCaseName)
//     //                             .ConfigureAwait(false);
//     //                         
//     //                         var result = await test
//     //                             .Run(testFixture, parameters)
//     //                             .ConfigureAwait(false);
//     //
//     //                         if (result is Failed failure)
//     //                             operation.Abandon(failure.Reason);
//     //                         else
//     //                             operation.Complete();
//     //
//     //                         await testFixture
//     //                             .DisposeAsync(testCaseName)
//     //                             .ConfigureAwait(false);
//     //                     }
//     //                 }
//     //             }
//     //         }
//     //     }
//     // }
// }
//
// class KrimsonTestDiscovery : IDiscovery {
//     public IEnumerable<Type> TestClasses(IEnumerable<Type> concreteClasses) =>
//         concreteClasses.Where(x => x.IsAssignableTo(typeof(ITestFixture)) || x.Has<TestFixtureAttribute>());
//
//     public IEnumerable<MethodInfo> TestMethods(IEnumerable<MethodInfo> publicMethods) =>
//         publicMethods
//             .Where(x => x.Has<TestAttribute>() || x.Has<TestCaseAttribute>() || x.Has<InlineDataAttribute>())
//             .Shuffle();
// }
//
// class KrimsonTestExecution : IExecution {
//     Func<LoggerConfiguration, LoggerConfiguration> ConfigureLogger { get; }
//
//     public KrimsonTestExecution(Action<LoggerConfiguration> configureLogger) {
//         ConfigureLogger = loggerConfiguration => {
//             configureLogger(loggerConfiguration);
//             return loggerConfiguration;
//         };
//     }
//     
//     public async Task Run(TestSuite testSuite) {
//         Log.Logger = ConfigureLogger(new LoggerConfiguration().MinimumLevel.Debug())
//             .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
//             .Enrich.FromLogContext()
//             .Enrich.WithThreadId()
//             .Enrich.WithExceptionDetails()
//             .Enrich.WithProperty("SourceContext", nameof(Fixie))
//             .WriteTo.Logger(
//                 logger => logger
//                     .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
//                     .WriteTo.Seq("http://localhost:5341")
//             )
//             .WriteTo.Logger(
//                 logger => logger
//                     //.Filter.ByExcluding(Matching.FromSource(nameof(Fixie)))
//                     .WriteTo.Console(theme: AnsiConsoleTheme.Literate, outputTemplate: LogOutputTemplate, applyThemeToRedirectedOutput: true)
//             )
//             .CreateLogger();
//         
//         try {
//             var testRunId = Guid.NewGuid().ToString();
//             
//             using (LogContext.PushProperty("TestRunId", testRunId)) {
//                 Log.Verbose("starting test run {TestRunId}", testRunId);
//                 
//                 foreach (var testClass in testSuite.TestClasses)
//                 foreach (var test in testClass.Tests) {
//                     await Execute(test, testClass).ConfigureAwait(false);
//                 }
//             }
//         }
//         finally {
//             Log.CloseAndFlush();
//         }
//
//         static async Task Execute(Test test, TestClass testClass) {
//             if (!test.HasParameters)
//                 await RunTest().ConfigureAwait(false);
//             else {
//                 foreach (var parameters in test.GetAll<InlineDataAttribute>().Select(x => x.Parameters))
//                     await RunTest(parameters).ConfigureAwait(false);
//             }
//
//             async Task RunTest(params object[] parameters) {
//                 var testCaseName = TestCaseName.From(test, parameters);
//                 
//                 Log.ForContext("SourceContext", nameof(Fixie))
//                     .Verbose("Test {TestCaseName} {Operation}", testCaseName, "executing");
//                 
//                 ILogEventEnricher[] props = {
//                     new PropertyEnricher("SourceContext", testCaseName),
//                     new PropertyEnricher("TestClass", testClass.Type.Name),
//                     new PropertyEnricher("TestName", test.Name),
//                     new PropertyEnricher("TestCaseName", testCaseName)
//                 };
//
//                 using (LogContext.Push(props)) {
//                     var operation = Log
//                         .ForContext("SourceContext", nameof(Fixie))
//                         .OperationAt(LogEventLevel.Information, LogEventLevel.Error)
//                         .Begin("Test {TestCaseName}", testCaseName);
//
//                     using (operation) {
//                         var testFixture = testClass.CreateTestFixtureInstance();
//
//                         await testFixture
//                             .InitializeAsync(testCaseName)
//                             .ConfigureAwait(false);
//                         
//                         var result = await test
//                             .Run(testFixture, parameters)
//                             .ConfigureAwait(false);
//
//                         if (result is Failed failure)
//                             operation.Abandon(failure.Reason);
//                         else
//                             operation.Complete();
//
//                         await testFixture
//                             .DisposeAsync(testCaseName)
//                             .ConfigureAwait(false);
//                     }
//                 }
//             }
//         }
//     }
// }
//
// static class TestClassExtensions {
//     public static ITestFixture CreateTestFixtureInstance(this TestClass testClass) {
//         var context = CreateTestContextInstance(testClass);
//         
//         return (ITestFixture)testClass.Construct(context);
//         
//         static ITestContext? CreateTestContextInstance(TestClass testClass) {
//             var contextType = testClass.Type.GetInterfaces()
//                 .FirstOrDefault(type => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ITestFixture<>))
//                 ?.GetGenericArguments()[0];
//
//             return contextType is not null ? (ITestContext)CreateInstance(contextType)! : null;
//         }
//     }
//   
// }
//
// static class TestFixtureExtensions {
//     public static async Task InitializeAsync(this ITestFixture fixture, string testCaseName) {
//         var contextDisplayName = fixture.TestContext?.GetType().Name;
//
//         var template = contextDisplayName is not null
//             ? "Test {TestCaseName} {ContextName} {Operation}"
//             : "Test {TestCaseName} {Operation}";
//
//         try {
//       
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(template, testCaseName, contextDisplayName, "initializing");
//
//             await fixture.InitializeAsync().ConfigureAwait(false);
//
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(template, testCaseName, contextDisplayName, "initialized");
//         }
//         catch (Exception ex) {
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(ex, template, testCaseName, contextDisplayName, "error");
//         }
//     }
//
//     public static async Task DisposeAsync(this ITestFixture fixture, string testCaseName) {
//           var contextDisplayName = fixture.TestContext?.GetType().Name;
//
//         var template = contextDisplayName is not null
//             ? "Test {TestCaseName} {ContextName} {Operation}"
//             : "Test {TestCaseName} {Operation}";
//
//         try {
//       
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(template, testCaseName, contextDisplayName, "disposing");
//
//             await fixture.InitializeAsync().ConfigureAwait(false);
//
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(template, testCaseName, contextDisplayName, "disposed");
//         }
//         catch (Exception ex) {
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(ex, template, testCaseName, contextDisplayName, "error");
//         }
//     }
// }