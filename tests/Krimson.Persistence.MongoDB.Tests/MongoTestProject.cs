using Krimson.Fixie;
using Serilog;
using Serilog.Events;

namespace Krimson.Persistence.MongoDB.Tests;

public class MongoTestProject : DefaultTestProject {
    //static KrimsonTestProject() => Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "test");
    
    public override LoggerConfiguration ConfigureLogger(LoggerConfiguration configuration) =>
        configuration.WriteTo.Logger(
            logger => logger
                .MinimumLevel.Override(nameof(Fixie), LogEventLevel.Verbose)
                .MinimumLevel.Override("ConfluentProducerLogger", LogEventLevel.Information)
                .MinimumLevel.Override("ConfluentProcessorLogger", LogEventLevel.Information)
                .WriteTo.Seq("http://localhost:5341")
        );
}