namespace Krimson.Connectors;

public class PullSourceConnectorHost<T> : BackgroundService  where T : DataSource<PullSourceContext> {
    public PullSourceConnectorHost(T source, IServiceProvider services, TimeSpan? backoffTime = null) {
        Source      = source;
        Services    = services;
        BackoffTime = backoffTime ?? GetBackoffTime(GetType());

        static TimeSpan GetBackoffTime(Type type) => 
            (BackOffTimeAttribute?)Attribute.GetCustomAttribute(type, typeof(BackOffTimeAttribute)) ?? TimeSpan.FromSeconds(30);
    }

    T                Source      { get; }
    IServiceProvider Services    { get; }
    TimeSpan         BackoffTime { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var context = new PullSourceContext(Services, stoppingToken);
        while (!context.CancellationToken.IsCancellationRequested) {
            await Source.Process(context).ConfigureAwait(false);
            await Task.Delay(BackoffTime, context.CancellationToken).ConfigureAwait(false);
        }
    }
}