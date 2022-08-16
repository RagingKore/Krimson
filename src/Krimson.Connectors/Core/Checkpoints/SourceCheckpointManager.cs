using Krimson.Readers;

namespace Krimson.Connectors.Checkpoints;

public class SourceCheckpointManager {
    public SourceCheckpointManager(KrimsonReader reader) {
        Reader      = reader;
        Checkpoints = new();
    }

    KrimsonReader                        Reader      { get; }
    Dictionary<string, SourceCheckpoint> Checkpoints { get; }

    static readonly object Locker = new object();
  
    public async ValueTask<SourceCheckpoint> GetCheckpoint(string topic, CancellationToken cancellationToken) {
        lock (Locker) {
            if (Checkpoints.TryGetValue(topic, out var checkpoint))
                return checkpoint;
        }
    
        var loadedCheckpoint = await Reader
            .LoadCheckpoint(topic, cancellationToken)
            .ConfigureAwait(false);
    
        lock (Locker) {
            return Checkpoints[topic] = loadedCheckpoint;
        }
    }
    
    public SourceCheckpoint TrackCheckpoint(SourceCheckpoint checkpoint) {
        lock (Locker) {
            return Checkpoints[checkpoint.RecordId.Topic] = checkpoint;    
        }
    }
}