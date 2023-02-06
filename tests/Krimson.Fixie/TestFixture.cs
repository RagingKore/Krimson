using JetBrains.Annotations;

namespace Krimson.Fixie;

public interface ITestFixture : IAsyncLifetime {
    ITestContext? TestContext { get; set; }
}

public interface ITestFixture<out T> : ITestFixture where T : ITestContext, new() {
    T Context { get; }
}

public abstract class TestFixture : AsyncLifetime, ITestFixture {
    protected TestFixture(ITestContext testContext) => ((ITestFixture)this).TestContext = testContext;

    ITestContext? ITestFixture.TestContext { get; set; }
}

public abstract class TestFixture<T> : TestFixture, ITestFixture<T> where T : ITestContext, new() {
    protected TestFixture(T context) : base(context) { }

    public T Context => (T)((ITestFixture)this).TestContext!;
}