namespace Krimson.Connectors;

class KrimsonPeriodicSourceConnectorService<TSourceConnector> : BackgroundService where TSourceConnector : KrimsonPeriodicSourceConnector {
    public KrimsonPeriodicSourceConnectorService(TSourceConnector connector, IServiceProvider services) {
        Connector = connector;
        Services  = services;
    }

    TSourceConnector Connector { get; }
    IServiceProvider Services  { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var context = new KrimsonPeriodicSourceConnectorContext(Services, stoppingToken);
        
        await Connector
            .Execute(context)
            .ConfigureAwait(false);
    }
}