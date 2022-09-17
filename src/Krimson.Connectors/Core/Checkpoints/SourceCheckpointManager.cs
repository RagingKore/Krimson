using Krimson.Readers;
using static System.DateTimeOffset;
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

        if (loadedCheckpoint == SourceCheckpoint.None) {
            Log.Information("Checkpoint not found");
        }
        else {
            Log.Information(
                "Checkpoint loaded: {EventTime} {Topic} [{Partition}] @ {Offset}",
                loadedCheckpoint.Timestamp, loadedCheckpoint.RecordId.Topic, 
                loadedCheckpoint.RecordId.Partition, loadedCheckpoint.RecordId.Offset 
            );
        }
       

        return Checkpoints[topic] = loadedCheckpoint;
    }
    
    public SourceCheckpoint TrackCheckpoint(SourceCheckpoint checkpoint) {
        
        Checkpoints[checkpoint.RecordId.Topic] = checkpoint;
        
        Log.Information(
            "Checkpoint tracked: {EventTime} {Topic} [{Partition}] @ {Offset}",
            checkpoint.Timestamp, checkpoint.RecordId.Topic, 
            checkpoint.RecordId.Partition, checkpoint.RecordId.Offset 
        );

        return checkpoint;
    }
}