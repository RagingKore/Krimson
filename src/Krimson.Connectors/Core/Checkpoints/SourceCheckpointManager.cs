using Krimson.Readers;
using static Serilog.Log;

namespace Krimson.Connectors.Checkpoints;

public class SourceCheckpointManager {
    static readonly Serilog.ILogger Log = ForContext<SourceCheckpointManager>();
    
    public SourceCheckpointManager(KrimsonReader reader) {
        Reader      = reader;
        Checkpoints = new();
    }

    KrimsonReader                        Reader      { get; }
    Dictionary<string, SourceCheckpoint> Checkpoints { get; }

    public async ValueTask<SourceCheckpoint> GetCheckpoint(string topic, CancellationToken cancellationToken) {
        if (Checkpoints.TryGetValue(topic, out var checkpoint))
            return checkpoint;
        
        var loadedCheckpoint = await Reader
            .LoadCheckpoint(topic, cancellationToken)
            .ConfigureAwait(false);
    
        Log.Information(
            "checkpoint loaded {Topic} [{Partition}] @ {Offset} :: {EventTime:O}",
            loadedCheckpoint.RecordId.Topic, loadedCheckpoint.RecordId.Partition,
            loadedCheckpoint.RecordId.Offset, loadedCheckpoint.Timestamp
        );

        return Checkpoints[topic] = loadedCheckpoint;
    }
    
    public SourceCheckpoint TrackCheckpoint(SourceCheckpoint checkpoint) => 
        Checkpoints[checkpoint.RecordId.Topic] = checkpoint;
}