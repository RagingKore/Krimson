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
// public abstract class KrimsonTestProject : ITestProject {
//     const string LogOutputTemplate = "[{Timestamp:HH:mm:ss.fff} {Level:u3}] ({ThreadId:000}) {SourceContext}{NewLine}{Message}{NewLine}{Exception}";
//
//     public void Configure(TestConfiguration configuration, TestEnvironment environment) =>
//         configuration.Conventions.Add(new CustomDiscovery(), new CustomExecution(ConfigureLogger));
//
//     public virtual void ConfigureLogger(LoggerConfiguration configuration) { }
//
//     class CustomDiscovery : IDiscovery {
//         static readonly string[] LifecycleMethods = {
//             "InitializeAsync",
//             "DisposeAsync"
//         };
//
//         public IEnumerable<Type> TestClasses(IEnumerable<Type> concreteClasses) => 
//             concreteClasses.Where(x => x.Name.EndsWith("Tests") || x.Has<TestClassAttribute>());
//
//         public IEnumerable<MethodInfo> TestMethods(IEnumerable<MethodInfo> publicMethods) =>
//             publicMethods
//                 .Where(x => !LifecycleMethods.Contains(x.Name))
//                 .Where(x => x.Has<TestAttribute>() || x.Has<InlineDataAttribute>())
//                 .Shuffle();
//     }
//
//     class CustomExecution : IExecution {
//         Func<LoggerConfiguration, LoggerConfiguration> ConfigureLogger { get; }
//
//         public CustomExecution(Action<LoggerConfiguration> configureLogger) {
//             ConfigureLogger = loggerConfiguration => {
//                 configureLogger(loggerConfiguration);
//                 return loggerConfiguration;
//             };
//         }
//         
//         public async Task Run(TestSuite testSuite) {
//             Log.Logger = ConfigureLogger(new LoggerConfiguration().MinimumLevel.Debug())
//                 .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
//                 .Enrich.FromLogContext()
//                 .Enrich.WithThreadId()
//                 .Enrich.WithExceptionDetails()
//                 .Enrich.WithProperty("SourceContext", nameof(Fixie))
//                 .WriteTo.Logger(
//                     logger => logger
//                         .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
//                         .WriteTo.Seq("http://localhost:5341")
//                 )
//                 .WriteTo.Logger(
//                     logger => logger
//                         //.Filter.ByExcluding(Matching.FromSource(nameof(Fixie)))
//                         .WriteTo.Console(theme: AnsiConsoleTheme.Literate, outputTemplate: LogOutputTemplate, applyThemeToRedirectedOutput: true)
//                 )
//                 .CreateLogger();
//             
//             try {
//                 var testRunId = Guid.NewGuid().ToString();
//                 
//                 using (LogContext.PushProperty("TestRunId", testRunId)) {
//                     Log.Verbose("starting test run {TestRunId}", testRunId);
//                     
//                     foreach (var testClass in testSuite.TestClasses)
//                     foreach (var test in testClass.Tests) {
//                         await Execute(test, testClass).ConfigureAwait(false);
//                     }
//                 }
//             }
//             finally {
//                 Log.CloseAndFlush();
//             }
//
//             static async Task Execute(Test test, TestClass testClass) {
//                 if (!test.HasParameters)
//                     await RunTest().ConfigureAwait(false);
//                 else {
//                     foreach (var parameters in test.GetAll<InlineDataAttribute>().Select(x => x.Parameters))
//                         await RunTest(parameters).ConfigureAwait(false);
//                 }
//
//                 async Task RunTest(params object[] parameters) {
//                     var testCaseName = TestCaseName.From(test, parameters);
//                     
//                     Log.ForContext("SourceContext", nameof(Fixie))
//                         .Verbose("Test {TestCaseName} {Operation}", testCaseName, "executing");
//                     
//                     ILogEventEnricher[] props = {
//                         new PropertyEnricher("SourceContext", testCaseName),
//                         new PropertyEnricher("TestClass", testClass.Type.Name),
//                         new PropertyEnricher("TestName", test.Name),
//                         new PropertyEnricher("TestCaseName", testCaseName)
//                     };
//
//                     using (LogContext.Push(props)) {
//                         var operation = Log
//                             .ForContext("SourceContext", nameof(Fixie))
//                             .OperationAt(LogEventLevel.Information, LogEventLevel.Error)
//                             .Begin("Test {TestCaseName}", testCaseName);
//
//                         using (operation) {
//                             var (testClassInstance, testContextInstance) = testClass.CreateInstances();
//
//                             if (testContextInstance is not null) {
//                                 await testContextInstance
//                                     .InitializeAsync(testCaseName)
//                                     .ConfigureAwait(false);
//                             }
//                             else {
//                                 await testClassInstance
//                                     .InitializeAsync(testCaseName)
//                                     .ConfigureAwait(false);
//                             }
//                             
//                             var result = await test
//                                 .Run(testClassInstance, parameters)
//                                 .ConfigureAwait(false);
//
//                             if (result is Failed failure)
//                                 operation.Abandon(failure.Reason);
//                             else
//                                 operation.Complete();
//
//                             if (testContextInstance is not null) {
//                                 await testContextInstance
//                                     .DisposeAsync(testCaseName)
//                                     .ConfigureAwait(false);
//                             }
//                             else {
//                                 await testClassInstance
//                                     .DisposeAsync(testCaseName)
//                                     .ConfigureAwait(false);
//                             }
//                         }
//                     }
//
//                     // using (LogContext.Push(props)) {
//                     //     var testClassInstance = testClass.AutoConstruct();
//                     //
//                     //     await testClass
//                     //         .InitializeAsync(testClassInstance, testCaseName)
//                     //         .ConfigureAwait(false);
//                     //
//                     //     var operation = Log
//                     //         .ForContext("SourceContext", nameof(Fixie))
//                     //         .OperationAt(LogEventLevel.Information, LogEventLevel.Error)
//                     //         .Begin("Test {TestCaseName}", testCaseName);
//                     //
//                     //     using (operation) {
//                     //         var result = await test
//                     //             .Run(testClassInstance, parameters)
//                     //             .ConfigureAwait(false);
//                     //
//                     //         if (result is Failed failure)
//                     //             operation.Abandon(failure.Reason);
//                     //         else
//                     //             operation.Complete();
//                     //     }
//                     //
//                     //     await testClass
//                     //         .DisposeAsync(testClassInstance, testCaseName)
//                     //         .ConfigureAwait(false);
//                     // }
//                 }
//             }
//         }
//     }
// }
// //
// // class AsyncLifetimeWrapper : IAsyncLifetime {
// //     public AsyncLifetimeWrapper(IAsyncLifetime lifetime, string testCaseName) {
// //         Lifetime     = lifetime;
// //         TestCaseName = testCaseName;
// //     }
// //
// //     IAsyncLifetime Lifetime     { get; }
// //     string         TestCaseName { get; }
// //   
// //     public async ValueTask InitializeAsync() {
// //         try {
// //             Log.ForContext("SourceContext", nameof(Fixie))
// //                 .Verbose("Test {TestCaseName} {Operation}", TestCaseName, "initializing");
// //
// //             await Lifetime
// //                 .InitializeAsync()
// //                 .ConfigureAwait(false);
// //
// //             Log.ForContext("SourceContext", nameof(Fixie)).Verbose("Test {TestCaseName} {Operation}", TestCaseName, "initialized");
// //         }
// //         catch (Exception ex) {
// //             Log.ForContext("SourceContext", nameof(Fixie))
// //                 .Verbose(ex, "Test {TestCaseName} {Operation}", TestCaseName, "error");
// //         }
// //     }
// //     
// //     public async ValueTask DisposeAsync() {
// //         try {
// //             Log.ForContext("SourceContext", nameof(Fixie))
// //                 .Verbose("Test {TestCaseName} {Operation}", TestCaseName, "disposing");
// //
// //             await Lifetime
// //                 .InitializeAsync()
// //                 .ConfigureAwait(false);
// //
// //             Log.ForContext("SourceContext", nameof(Fixie))
// //                 .Verbose("Test {TestCaseName} {Operation}", TestCaseName, "disposed");
// //         }
// //         catch (Exception ex) {
// //             Log.ForContext("SourceContext", nameof(Fixie))
// //                 .Verbose(ex, "Test {TestCaseName} {Operation}", TestCaseName, "error");
// //         }
// //     }
// // }
//
// static class TestClassExtensions {
//     // public static IAsyncLifetime TryCreateTestContext(this TestClass testClass, string testCaseName) {
//     //     var contextType = testClass.Type.GetInterfaces()
//     //         .FirstOrDefault(type => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ITestFixture<>))?.GetGenericArguments()[0];
//     //
//     //     return contextType is not null 
//     //         ? new AsyncLifetimeWrapper((IAsyncLifetime)CreateInstance(contextType)!, testCaseName)
//     //         : AsyncLifetime.Default;
//     // }
//     //
//     // public static IAsyncLifetime TryCreateTestFixture(this TestClass testClass, string testCaseName) {
//     //     var contextType = testClass.Type.GetInterfaces()
//     //         .FirstOrDefault(type => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ITestFixture<>))?.GetGenericArguments()[0];
//     //
//     //     return contextType is not null
//     //         ? new AsyncLifetimeWrapper((IAsyncLifetime)CreateInstance(contextType)!, testCaseName)
//     //         : AsyncLifetime.Default;
//     // }
//     //
//     public static (IAsyncLifetime Fixture, IAsyncLifetime? Context) CreateInstances(this TestClass testClass) {
//         var context = CreateTestContext(testClass);
//         
//         return ((IAsyncLifetime)testClass.Construct(context), context);
//         
//         static IAsyncLifetime? CreateTestContext(TestClass testClass) {
//             var contextType = testClass.Type.GetInterfaces()
//                 .FirstOrDefault(type => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ITestFixture<>))?.GetGenericArguments()[0];
//
//             return contextType is not null ? (IAsyncLifetime)CreateInstance(contextType)! : null;
//
//             // var fixture = testClass.Type.GetInterfaces()
//             //     .Where(type => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IClassFixture<>))
//             //     .Select(interfaceType => CreateInstance(interfaceType.GetGenericArguments()[0])!)
//             //     .ToArray();
//         }
//     }
//
//     // public static object AutoConstruct2(this TestClass testClass) {
//     //     return testClass.Construct(GetConstructorParameters(testClass));
//     //
//     //     static object[] GetConstructorParameters(TestClass testClass) =>
//     //         testClass.Type.GetInterfaces()
//     //             .Where(type => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IClassFixture<>))
//     //             .Select(interfaceType => CreateInstance(interfaceType.GetGenericArguments()[0])!)
//     //             .ToArray();
//     // }
//
//     public static async Task InitializeAsync(this IAsyncLifetime source, string testCaseName) {
//         try {
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose("Test {TestCaseName} {Operation}", testCaseName, "initializing");
//
//             await source.InitializeAsync().ConfigureAwait(false);
//
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose("Test {TestCaseName} {Operation}", testCaseName, "initialized");
//         }
//         catch (Exception ex) {
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(ex, "Test {TestCaseName} {Operation}", testCaseName, "error");
//         }
//     }
//
//     public static async Task DisposeAsync(this IAsyncLifetime source, string testCaseName) {
//         try {
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose("Test {TestCaseName} {Operation}", testCaseName, "disposing");
//
//             await source.InitializeAsync().ConfigureAwait(false);
//             
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose("Test {TestCaseName} {Operation}", testCaseName, "disposed");
//         }
//         catch (Exception ex) {
//             Log.ForContext("SourceContext", nameof(Fixie))
//                 .Verbose(ex, "Test {TestCaseName} {Operation}", testCaseName, "error");
//         }
//     }
//     
//     //
//     // public static async Task InitializeAsync(this TestClass testClass, object instance, string testCaseName) {
//     //     var method = testClass.Type.GetMethod("InitializeAsync");
//     //
//     //     if (method is null)
//     //         return;
//     //
//     //     try {
//     //         Log.ForContext("SourceContext", nameof(Fixie))
//     //             .Verbose("Test {TestCaseName} {Operation}", testCaseName, "disposing");
//     //         
//     //         await method.Call(instance).ConfigureAwait(false);
//     //     }
//     //     catch (Exception ex) {
//     //         Log.ForContext("SourceContext", nameof(Fixie))
//     //             .Verbose(ex, "Test {TestCaseName} {Operation}", testCaseName, "error");
//     //     }
//     //     finally {
//     //         Log.ForContext("SourceContext", nameof(Fixie))
//     //             .Verbose("Test {TestCaseName} {Operation}", testCaseName, "disposed");
//     //     }
//     // }
//     //
//     // public static async Task DisposeAsync(this TestClass testClass, object instance, string testCaseName) {
//     //     var method = testClass.Type.GetMethod("DisposeAsync");
//     //
//     //     if (method is null)
//     //         return;
//     //
//     //     try {
//     //         Log.ForContext("SourceContext", nameof(Fixie))
//     //             .Verbose("Test {TestCaseName} {Operation}", testCaseName, "disposing");
//     //         
//     //         await method.Call(instance).ConfigureAwait(false);
//     //     }
//     //     catch (Exception ex) {
//     //         Log.ForContext("SourceContext", nameof(Fixie))
//     //             .Verbose(ex, "Test {TestCaseName} {Operation}", testCaseName, "error");
//     //     }
//     //     finally {
//     //         Log.ForContext("SourceContext", nameof(Fixie))
//     //             .Verbose("Test {TestCaseName} {Operation}", testCaseName, "disposed");
//     //     }
//     //     
//     // }
// }