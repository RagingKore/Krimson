using Krimson.Readers;

namespace Krimson.Connectors.Checkpoints;

class SourceCheckpointManager {
    public SourceCheckpointManager(KrimsonReader reader) {
        Reader      = reader;
        Checkpoints = new();
    }

    KrimsonReader                        Reader      { get; }
    Dictionary<string, SourceCheckpoint> Checkpoints { get; }

    public async ValueTask<SourceCheckpoint> GetCheckpoint(string topic, CancellationToken cancellationToken) {
        if (Checkpoints.TryGetValue(topic, out var checkpoint))
            return checkpoint;

        checkpoint = await Reader
            .LoadCheckpoint(topic, cancellationToken)
            .ConfigureAwait(false);

        return Checkpoints[topic] = checkpoint;
    }
}