namespace Krimson.Fixie;

public interface IAsyncLifetime : IAsyncDisposable {
    public ValueTask InitializeAsync();
}

public abstract class AsyncLifetime : IAsyncLifetime {
    public static readonly AsyncLifetime Default = new DefaultAsyncLifetime();
    
    ValueTask IAsyncLifetime.InitializeAsync() => SetUp();
    ValueTask IAsyncDisposable.DisposeAsync()  => CleanUp();

    protected virtual ValueTask SetUp()   => ValueTask.CompletedTask;
    protected virtual ValueTask CleanUp() => ValueTask.CompletedTask;

    class DefaultAsyncLifetime : AsyncLifetime { }
}