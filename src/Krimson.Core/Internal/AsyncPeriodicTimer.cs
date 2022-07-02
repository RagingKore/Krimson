namespace Krimson;

delegate Task OnPeriodicTimerTick(bool isLastTick, CancellationToken cancellationToken);

sealed class AsyncPeriodicTimer : IAsyncDisposable {
    public AsyncPeriodicTimer(TimeSpan period, OnPeriodicTimerTick onTick) {
        Period = period;
        OnTick = onTick;
    }

    TimeSpan            Period { get; }
    OnPeriodicTimerTick OnTick { get; }
    PeriodicTimer?      Timer  { get; set; }

    public async Task Start(CancellationToken stoppingToken = default) {
        Timer = new PeriodicTimer(Period);

        await Task.Yield();

        while (await Timer.WaitForNextTickAsync(stoppingToken))
            await OnTick(false, stoppingToken);

        await OnTick(true, stoppingToken);
    }

    public ValueTask DisposeAsync() {
        Timer?.Dispose();
        return ValueTask.CompletedTask;
    }
}