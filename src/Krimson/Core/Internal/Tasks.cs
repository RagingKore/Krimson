namespace Krimson;

static class Tasks {
    public static async Task SafeDelay(TimeSpan delay, CancellationToken cancellationToken) {
        try {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) {
            // fail gracefully
        }
    }
}