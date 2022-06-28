namespace Krimson.Fixie;

public interface ITestContext : IAsyncLifetime { }

public abstract class TestContext : AsyncLifetime, ITestContext{ }