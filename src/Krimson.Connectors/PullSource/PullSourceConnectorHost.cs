using Microsoft.Extensions.Hosting;

namespace Krimson.Connectors;

public class PullSourceConnectorHost<T> : BackgroundService where T : DataSourceConnector<PullSourceContext> {
    public PullSourceConnectorHost(T source, IServiceProvider services, TimeSpan? backoffTime = null) {
        Source      = source;
        Services    = services;
        BackoffTime = backoffTime ?? GetBackoffTime(typeof(T));

        static TimeSpan GetBackoffTime(Type type) => 
            (BackOffTimeSecondsAttribute?)Attribute.GetCustomAttribute(type, typeof(BackOffTimeSecondsAttribute)) ?? TimeSpan.FromSeconds(30);
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