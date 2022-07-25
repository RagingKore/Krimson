using Microsoft.Extensions.Hosting;

namespace Krimson.Connectors;

public class BackgroundPullSourceConnectorService<T> : BackgroundService where T : PullSourceConnector {
    public BackgroundPullSourceConnectorService(T connector) => Connector = connector;

    T Connector { get; }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Connector.Execute(stoppingToken);
}